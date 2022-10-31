// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlpexporter

import (
	"context"
	"fmt"
	"sync"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	batchEvent "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// High-level TODOs:
// TODO: Use the MAX_CONNECTION_AGE and MAX_CONNECTION_AGE_GRACE settings.

// arrowExporter is 1:1 with exporter, isolates arrow-specific
// functionality.
type arrowExporter struct {
	// settings contains Arrow-specific parameters.
	settings *ArrowSettings

	// telemetry includes logger, tracer, meter.
	telemetry component.TelemetrySettings

	// grpcOptions includes options used by the unary RPC methods,
	// e.g., WaitForReady.
	grpcOptions []grpc.CallOption

	// client is created from the exporter's gRPC ClientConn.
	client arrowpb.ArrowStreamServiceClient

	// ready prioritizes streams that are ready to send
	ready streamPrioritizer

	// returning is used to pass broken, gracefully-terminated,
	// and otherwise to the stream controller.
	returning chan *arrowStream

	// cancel cancels the background context of this
	// arrowExporter, used for shutdown.
	cancel context.CancelFunc

	// wg counts one per active goroutine belonging to all strings
	// of this exporter.  The wait group has Add(1) called before
	// starting goroutines so that they can be properly waited for
	// in shutdown(), so the pattern is:
	//
	//   wg.Add(1)
	//   go func() {
	//     defer wg.Done()
	//     ...
	//   }()
	wg sync.WaitGroup
}

// writeItem is passed from the sender (a pipeline consumer) to the
// stream writer, which is not bound by the sender's context.
type writeItem struct {
	// records is a ptrace.Traces, plog.Logs, or pmetric.Metrics
	records interface{}
	// errCh is used by the stream reader to unblock the sender
	errCh chan error
}

// arrowStream is 1:1 with gRPC stream.
type arrowStream struct {
	// client uses the exporter's grpc.ClientConn.
	client arrowpb.ArrowStreamService_ArrowStreamClient

	// toWrite is passes a batch from the sender to the stream writer, which
	// includes a dedicated channel for the response.
	toWrite chan writeItem

	// producer is exclusive to the holder of the stream.
	producer *batchEvent.Producer

	// cancel cancels the stream context.
	cancel context.CancelFunc

	// lock protects waiters.
	lock sync.Mutex

	// waiters is the response channel for each active batch.
	waiters map[string]chan error
}

// startArrowExporter creates the background context used by all streams and
// starts a stream controller, which initializes the initial set of streams.
func startArrowExporter(ctx context.Context, settings *ArrowSettings, telemetry component.TelemetrySettings, clientConn *grpc.ClientConn, grpcOptions []grpc.CallOption) *arrowExporter {
	ctx, cancel := context.WithCancel(ctx)

	ae := &arrowExporter{
		settings:    settings,
		telemetry:   telemetry,
		grpcOptions: grpcOptions,
		client:      arrowpb.NewArrowStreamServiceClient(clientConn),
		ready:       newStreamPrioritizer(settings),
		returning:   make(chan *arrowStream, settings.NumStreams),
		cancel:      cancel,
	}
	ae.wg.Add(1)
	go ae.runStreamController(ctx)

	return ae
}

// runStreamController starts the initial set of streams, then waits for streams to
// terminate one at a time and restarts them.  If streams come back with a nil
// client (meaning that OTLP+Arrow was not supported by the endpoint), it will
// not be restarted.
func (ae *arrowExporter) runStreamController(bgctx context.Context) {
	defer ae.cancel()
	defer ae.wg.Done()

	running := ae.settings.NumStreams

	// Start the initial number of streams
	for i := 0; i < running; i++ {
		ae.wg.Add(1)
		go ae.runArrowStream(bgctx)
	}

	for {
		select {
		case stream := <-ae.returning:
			if stream.client != nil {
				// The stream closed or broken.  Restart it.
				ae.wg.Add(1)
				go ae.runArrowStream(bgctx)
				continue
			}
			// Otherwise, the stream never got started.  It was
			// downgraded and senders will use the standard OTLP path.
			running--

			// None of the streams were able to connect to
			// an Arrow endpoint.
			if running == 0 {
				ae.telemetry.Logger.Info("failed to establish OTLP+Arrow streaming, downgrading")
				ae.ready.downgrade()
			}

		case <-bgctx.Done():
			// We are shutting down.
			return
		}
	}
}

// runArrowStream begins one gRPC stream using a child of the background context.
// If the stream connection is successful, this goroutine starts another goroutine
// to call writeStream() and performs readStream() itself.  When the stream shuts
// down this call synchronously waits for and unblocks the consumers.
func (ae *arrowExporter) runArrowStream(bgctx context.Context) {
	ctx, cancel := context.WithCancel(bgctx)

	stream := &arrowStream{
		toWrite:  make(chan writeItem, 1),
		producer: batchEvent.NewProducer(),
		waiters:  map[string]chan error{},
		cancel:   cancel,
	}

	defer func() {
		ae.wg.Done()
		cancel()
		ae.returning <- stream
	}()

	sc, err := ae.client.ArrowStream(ctx, ae.grpcOptions...)
	if err != nil {
		// TODO: only when this is a permanent (e.g., "no such
		// method") error, downgrade to standard OTLP.
		// Returning with stream.client == nil signals the
		// lack of an Arrow stream endpoint.  When all the
		// streams return with .client == nil, the ready
		// channel will be closed.
		ae.telemetry.Logger.Error("cannnot start event stream", zap.Error(err))
		return
	}
	// Setting .client != nil indicates that the endpoint was valid,
	// streaming may start.  When this stream finishes, it will be
	// restarted.
	stream.client = sc

	var ww sync.WaitGroup

	ww.Add(1)
	ae.wg.Add(1)
	go ae.writeStream(ctx, stream, &ww)

	if err := ae.readStream(ctx, stream); err != nil {
		// TODO: should this log even an io.EOF error?
		ae.telemetry.Logger.Error("arrow recv", zap.Error(err))
	}

	// Wait for the writer to ensure that all waiters are known.
	ww.Wait()

	// The reader and writer have both finished; respond to any
	// outstanding waiters.
	for _, ch := range stream.waiters {
		// Note: exporterhelper will retry.
		// TODO: Would it be better to handle retry in this directly?
		ch <- status.Error(codes.Aborted, "stream is restarting")
	}
}

// getStream is called to get an available stream with the user's pipeline context.
func (ae *arrowExporter) getStream(ctx context.Context) (*arrowStream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case stream := <-ae.ready.readyChannel():
		return stream, nil
	}
}

// shutdown returns when all Arrow-associated goroutines have returned.
func (ae *arrowExporter) shutdown(ctx context.Context) error {
	ae.cancel()
	ae.wg.Wait()
	return nil
}

// setBatchChannel places a waiting consumer's batchID into the waiters map, where
// the stream reader may find it.
func (stream *arrowStream) setBatchChannel(batchID string, errCh chan error) {
	stream.lock.Lock()
	defer stream.lock.Unlock()

	stream.waiters[batchID] = errCh
}

// writeStream repeatedly places this stream into the next-available queue, then
// performs a blocking send().  This returns when the data is in the write buffer,
// the caller waiting on its error channel.
func (ae *arrowExporter) writeStream(ctx context.Context, stream *arrowStream, ww *sync.WaitGroup) {
	defer ww.Done()
	defer stream.cancel()

	for {
		// Note: this can't block b/c stream has capacity &
		// individual streams shut down synchronously.
		ae.ready.setReady(stream)

		// this can block, and if the context is canceled we
		// wait for the reader to find this stream.
		var wri writeItem
		select {
		case wri = <-stream.toWrite:
		case <-ctx.Done():
			// Because we did not <-stream.toWrite, there
			// is a potential sender race since the stream
			// is currently in the ready set.  Call
			// removeReady() in this case.
			ae.ready.removeReady(stream)
			return
		}
		// Note: For the two return statements below there is no potential
		// sender race because the stream is not available, as indicated by
		// the successful <-stream.toWrite.

		batch, err := stream.encode(wri.records)
		if err != nil {
			// TODO: Is this not permanent?  Another
			// sequence of data might not produce it.
			//
			// This is some kind of internal error.
			wri.errCh <- consumererror.NewPermanent(err)
			ae.telemetry.Logger.Error("arrow encode", zap.Error(err))
			return
		}

		stream.setBatchChannel(batch.BatchId, wri.errCh)

		if err := stream.client.Send(batch); err != nil {
			// The error will be sent to errCh during cleanup for this stream.
			ae.telemetry.Logger.Error("arrow send", zap.Error(err))
			return
		}
	}
}

// readStream repeatedly reads a batch status and releases the consumers waiting for
// a response.
func (ae *arrowExporter) readStream(ctx context.Context, stream *arrowStream) error {
	defer stream.cancel()

	for {
		resp, err := stream.client.Recv()
		if err != nil {
			return err
		}

		if err = stream.processBatchStatus(resp.Statuses); err != nil {
			return err
		}
	}
}

// getSenderChannels takes the stream lock and removes the corresonding sender
// channel for each BatchId.
func (stream *arrowStream) getSenderChannels(statuses []*arrowpb.StatusMessage) ([]chan error, error) {
	var err error

	fin := make([]chan error, len(statuses))

	stream.lock.Lock()
	defer stream.lock.Unlock()

	for idx, status := range statuses {
		ch, ok := stream.waiters[status.BatchId]
		if !ok {
			// Will break the stream.
			err = multierr.Append(err, fmt.Errorf("duplicate stream response: %s", status.BatchId))
			continue
		}
		delete(stream.waiters, status.BatchId)
		fin[idx] = ch
	}

	return fin, err
}

// processBatchStatus processes a single response from the server and unblocks the
// associated senders.
func (stream *arrowStream) processBatchStatus(statuses []*arrowpb.StatusMessage) error {
	fin, ret := stream.getSenderChannels(statuses)

	for idx, ch := range fin {
		status := statuses[idx]

		if status.StatusCode == arrowpb.StatusCode_OK {
			ch <- nil
			continue
		}
		var err error
		switch status.ErrorCode {
		case arrowpb.ErrorCode_UNAVAILABLE:
			// TODO: translate retry information into the form
			// exporterhelper recognizes.
			err = fmt.Errorf("destination unavailable: %s: %s", status.BatchId, status.ErrorMessage)
		case arrowpb.ErrorCode_INVALID_ARGUMENT:
			err = consumererror.NewPermanent(
				fmt.Errorf("invalid argument: %s: %s", status.BatchId, status.ErrorMessage))
		default:
			base := fmt.Errorf("unexpected stream response: %s: %s", status.BatchId, status.ErrorMessage)
			err = consumererror.NewPermanent(base)

			// Will break the stream.
			ret = multierr.Append(ret, base)
		}
		ch <- err
	}
	return ret
}

// sendAndWait submits a batch of records to be encoded and sent.  Meanwhile, this
// goroutine waits on the incoming context or for the asynchronous response to be
// received by the stream reader.
func (ae *arrowExporter) sendAndWait(ctx context.Context, stream *arrowStream, records interface{}) error {
	errCh := make(chan error, 1)
	stream.toWrite <- writeItem{
		records: records,
		errCh:   errCh,
	}

	// Note this ensures the caller's timeout is respected.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// encode produces the next batch of Arrow records.
func (stream *arrowStream) encode(records interface{}) (*arrowpb.BatchArrowRecords, error) {
	// Note!! This is a placeholder.  After this PR merges the
	// code base will be upgraded to the latest version of
	// github.com/f5/otel-arrow-adapter, which returns one
	// BatchArrowRecords per pdata Logs/Metrics/Traces.  The TODO below
	// will be addressed, i.e., we will not drop all but one
	// batch.
	var batch *arrowpb.BatchArrowRecords
	var err error
	switch data := records.(type) {
	case ptrace.Traces:
		batch, err = stream.producer.BatchArrowRecordsFrom(data)
	case plog.Logs:
		// TODO e.g., batch, err = stream.producer.BatchArrowRecordsFrom(data)
		// after https://github.com/f5/otel-arrow-adapter/pull/13.
	case pmetric.Metrics:
		// TODO: This will follow.
	}
	return batch, err
}

// streamPrioritizer is a placeholder for a configurable mechanism
// that selects the next stream to write.
type streamPrioritizer struct {
	// channel will be closed to downgrade to standard OTLP,
	// otherwise it returns the first-available.
	channel chan *arrowStream
}

// newStreamPrioritizer constructs a channel-based first-available prioritizer.
func newStreamPrioritizer(settings *ArrowSettings) streamPrioritizer {
	return streamPrioritizer{
		make(chan *arrowStream, settings.NumStreams),
	}
}

// downgrade indicates that streams are never going to be ready.  Note
// the caller is required to ensure that setReady() and removeReady()
// cannot be called concurrently; this is done by waiting for
// arrowStream.writeStream() calls to return before downgrading.
func (sp *streamPrioritizer) downgrade() {
	close(sp.channel)
}

// readyChannel returns channel to select a ready stream.  The caller
// is expected to select on this and ctx.Done() simultaneously.  If
// the exporter is downgraded, the channel will be closed.
func (sp *streamPrioritizer) readyChannel() chan *arrowStream {
	return sp.channel
}

// setReady marks this stream ready for use.
func (sp *streamPrioritizer) setReady(stream *arrowStream) {
	// Note: downgrade() can't be called concurrently.
	sp.channel <- stream
}

// removeReady removes this stream from the ready set, used in cases
// where the stream has broken unexpectedly.
func (sp *streamPrioritizer) removeReady(stream *arrowStream) {
	// Note: downgrade() can't be called concurrently.
	for {
		// Searching for this stream to get it out of the ready queue.
		select {
		case alternate := <-sp.channel:
			if alternate == stream {
				return
			}
			sp.channel <- alternate
		case wri := <-stream.toWrite:
			// A consumer got us first.  Note: exporterhelper will retry.
			// TODO: Would it be better to handle retry in this directly?
			wri.errCh <- status.Error(codes.Aborted, "stream is restarting")
		}
	}
}
