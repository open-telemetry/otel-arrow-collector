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

package arrow

import (
	"context"
	"fmt"
	"sync"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	arrowRecord "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type arrowProducer interface {
	BatchArrowRecordsFromTraces(ptrace.Traces) (*arrowpb.BatchArrowRecords, error)
	BatchArrowRecordsFromLogs(plog.Logs) (*arrowpb.BatchArrowRecords, error)
}

var _ arrowProducer = (*arrowRecord.Producer)(nil)

// Stream is 1:1 with gRPC stream.
type Stream struct {
	// producer is exclusive to the holder of the stream.
	producer arrowProducer

	// prioritizer has a reference to the stream, this allows it to be severed.
	prioritizer *streamPrioritizer

	// telemetry are a copy of the exporter's telemetry settings
	telemetry component.TelemetrySettings

	// client uses the exporter's grpc.ClientConn.
	client arrowpb.ArrowStreamService_ArrowStreamClient

	// toWrite is passes a batch from the sender to the stream writer, which
	// includes a dedicated channel for the response.
	toWrite chan writeItem

	// lock protects waiters.
	lock sync.Mutex

	// waiters is the response channel for each active batch.
	waiters map[string]chan error
}

// writeItem is passed from the sender (a pipeline consumer) to the
// stream writer, which is not bound by the sender's context.
type writeItem struct {
	// records is a ptrace.Traces, plog.Logs, or pmetric.Metrics
	records interface{}
	// errCh is used by the stream reader to unblock the sender
	errCh chan error
}

// newStream constructs a stream
func newStream(
	producer arrowProducer,
	prioritizer *streamPrioritizer,
	telemetry component.TelemetrySettings,
) *Stream {
	return &Stream{
		producer:    producer,
		prioritizer: prioritizer,
		telemetry:   telemetry,
		toWrite:     make(chan writeItem, 1),
		waiters:     map[string]chan error{},
	}
}

// setBatchChannel places a waiting consumer's batchID into the waiters map, where
// the stream reader may find it.
func (s *Stream) setBatchChannel(batchID string, errCh chan error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.waiters[batchID] = errCh
}

func (s *Stream) run(bgctx context.Context, client arrowpb.ArrowStreamServiceClient, grpcOptions []grpc.CallOption) {
	ctx, cancel := context.WithCancel(bgctx)
	defer cancel()

	sc, err := client.ArrowStream(ctx, grpcOptions...)
	if err != nil {
		// TODO: only when this is a permanent (e.g., "no such
		// method") error, downgrade to standard OTLP.
		// Returning with stream.client == nil signals the
		// lack of an Arrow stream endpoint.  When all the
		// streams return with .client == nil, the ready
		// channel will be closed.
		s.telemetry.Logger.Error("cannot start event stream", zap.Error(err))
		return
	}
	// Setting .client != nil indicates that the endpoint was valid,
	// streaming may start.  When this stream finishes, it will be
	// restarted.
	s.client = sc

	// ww is used to wait for the writer.  Since we wait for the writer,
	// the writer's goroutine is not added to exporter waitgroup (e.wg).
	var ww sync.WaitGroup

	ww.Add(1)
	go func() {
		defer ww.Done()
		defer cancel()
		s.write(ctx)
	}()

	if err := s.read(ctx); err != nil {
		s.telemetry.Logger.Error("arrow recv", zap.Error(err))
	}
	// Wait for the writer to ensure that all waiters are known.
	cancel()
	ww.Wait()

	// The reader and writer have both finished; respond to any
	// outstanding waiters.
	for _, ch := range s.waiters {
		// Note: exporterhelper will retry.
		// TODO: Would it be better to handle retry in this directly?
		ch <- status.Error(codes.Aborted, "stream is restarting")
	}
}

// write repeatedly places this stream into the next-available queue, then
// performs a blocking send().  This returns when the data is in the write buffer,
// the caller waiting on its error channel.
func (s *Stream) write(ctx context.Context) {
	for {
		// Note: this can't block b/c stream has capacity &
		// individual streams shut down synchronously.
		s.prioritizer.setReady(s)

		// this can block, and if the context is canceled we
		// wait for the reader to find this stream.
		var wri writeItem
		select {
		case wri = <-s.toWrite:
		case <-ctx.Done():
			// Because we did not <-stream.toWrite, there
			// is a potential sender race since the stream
			// is currently in the ready set.
			s.prioritizer.removeReady(s)
			return
		}
		// Note: For the two return statements below there is no potential
		// sender race because the stream is not available, as indicated by
		// the successful <-stream.toWrite.

		batch, err := s.encode(wri.records)
		if err != nil {
			// TODO: Is this not permanent?  Another
			// sequence of data might not produce it.
			//
			// This is some kind of internal error.
			wri.errCh <- consumererror.NewPermanent(err)
			s.telemetry.Logger.Error("arrow encode", zap.Error(err))
			return
		}

		// Let the receiver knows what to look for.
		s.setBatchChannel(batch.BatchId, wri.errCh)

		if err := s.client.Send(batch); err != nil {
			// The error will be sent to errCh during cleanup for this stream.
			s.telemetry.Logger.Error("arrow send", zap.Error(err))
			return
		}
	}
}

// read repeatedly reads a batch status and releases the consumers waiting for
// a response.
func (s *Stream) read(_ context.Context) error {
	// Note we do not use the context, the stream context might
	// cancel a call to Recv() but the call to processBatchStatus
	// is non-blocking.
	for {
		resp, err := s.client.Recv()
		if err != nil {
			return err
		}

		if err = s.processBatchStatus(resp.Statuses); err != nil {
			return err
		}
	}
}

// getSenderChannels takes the stream lock and removes the
// corresonding sender channel for each BatchId.  They are returned
// with the same index as the original status, for correlation.  Nil
// channels will be returned when there are errors locating the
// sender channel.
func (s *Stream) getSenderChannels(statuses []*arrowpb.StatusMessage) ([]chan error, error) {
	var err error

	fin := make([]chan error, len(statuses))

	s.lock.Lock()
	defer s.lock.Unlock()

	for idx, status := range statuses {
		ch, ok := s.waiters[status.BatchId]
		if !ok {
			// Will break the stream.
			err = multierr.Append(err, fmt.Errorf("duplicate stream response: %s", status.BatchId))
			continue
		}
		delete(s.waiters, status.BatchId)
		fin[idx] = ch
	}

	return fin, err
}

// processBatchStatus processes a single response from the server and unblocks the
// associated senders.
func (s *Stream) processBatchStatus(statuses []*arrowpb.StatusMessage) error {
	fin, ret := s.getSenderChannels(statuses)

	for idx, ch := range fin {
		if ch == nil {
			// In case getSenderChannels encounters a problem, the
			// channel is nil.
			continue
		}
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

// SendAndWait submits a batch of records to be encoded and sent.  Meanwhile, this
// goroutine waits on the incoming context or for the asynchronous response to be
// received by the stream reader.
func (s *Stream) SendAndWait(ctx context.Context, records interface{}) error {
	errCh := make(chan error, 1)
	s.toWrite <- writeItem{
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
func (s *Stream) encode(records interface{}) (_ *arrowpb.BatchArrowRecords, retErr error) {
	// Defensively, protect against panics in the Arrow producer function.
	defer func() {
		if err := recover(); err != nil {
			retErr = fmt.Errorf("panic in otel-arrow-adapter: %v", err)
		}
	}()
	var batch *arrowpb.BatchArrowRecords
	var err error
	switch data := records.(type) {
	case ptrace.Traces:
		batch, err = s.producer.BatchArrowRecordsFromTraces(data)
	case plog.Logs:
		batch, err = s.producer.BatchArrowRecordsFromLogs(data)
	case pmetric.Metrics:
		// TODO: This will follow.
		return nil, fmt.Errorf("unsupported OTLP type: metrics")
	default:
		return nil, fmt.Errorf("unsupported OTLP type: %T", records)
	}
	return batch, err
}
