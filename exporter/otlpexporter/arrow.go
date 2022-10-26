package otlpexporter

import (
	"context"
	"sync"

	arrowpb "github.com/lquerel/otel-arrow-adapter/api/collector/arrow/v1"
	batchEvent "github.com/lquerel/otel-arrow-adapter/pkg/otel/batch_event"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// arrowExporter is 1:1 with exporter, isolates arrow-specific
// functionality.
type arrowExporter struct {
	// exporter allows this to refer to all the settings.  Note
	// there is a pointer cycle, exporter refers to this object.
	exporter *exporter

	// client is created from the exporter's gRPC ClientConn.
	client arrowpb.EventsServiceClient

	// ready prioritizes streams that are ready to send
	ready streamPrioritizer

	// returning is used to pass broken, gracefully-terminated,
	// and otherwise to the stream controller.
	returning chan *arrowStream

	// cancel cancels the background context of this
	// arrowExporter, used for shutdown.
	cancel context.CancelFunc

	// wg counts one per active goroutine belonging to all strings
	// of this exporter.
	wg sync.WaitGroup
}

// streamPrioritizer is a placeholder for a configurable mechanism
// that selects the next strea, to write.
type streamPrioritizer struct {
	// channel will be closed to downgrade to standard OTLP,
	// otherwise it returns the first-available.
	channel chan *arrowStream
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
	client arrowpb.EventsService_EventStreamClient

	// toWrite is passes a batch from the sender to the stream writer, which
	// includes a dedicated channel for the response.
	toWrite chan writeItem

	// producer is exclusive to the holder of the stream
	producer *batchEvent.Producer

	// cancel cancels stream context
	cancel context.CancelFunc

	// lock protects waiters
	lock sync.Mutex

	// waiters is the response channel for each active batch.
	waiters map[string]chan error
}

// startArrowExporter creates the background context used by all streams and
// starts a stream controller, which initializes the initial set of streams.
func (e *exporter) startArrowExporter() *arrowExporter {
	bgctx, bgcancel := context.WithCancel(context.Background())
	bgctx = e.enhanceContext(bgctx)

	ae := &arrowExporter{
		exporter:  e,
		client:    arrowpb.NewEventsServiceClient(e.clientConn),
		ready:     e.newPrioritizer(),
		returning: make(chan *arrowStream, e.config.Arrow.NumStreams),
		cancel:    bgcancel,
	}
	ae.wg.Add(1)
	go ae.runStreamController(bgctx)

	return ae
}

// runStreamController starts the initial set of streams, then waits for streams to
// terminate one at a time and restarts them.  If streams come back with a nil
// client (meaning that OTLP+Arrow was not supported by the endpoint), it will
// not be restarted.
func (ae *arrowExporter) runStreamController(bgctx context.Context) {
	defer ae.cancel()
	defer ae.wg.Done()

	running := ae.exporter.config.Arrow.NumStreams

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

	sc, err := ae.client.EventStream(ctx, ae.exporter.callOptions...)
	if err != nil {
		// TODO: only when this is a permanent (e.g., "no such
		// method") error, downgrade to standard OTLP.
		// Returning with stream.client == nil signals the
		// lack of an Arrow stream endpoint.  When all the
		// streams return with .client == nil, the ready
		// channel will be closed.
		ae.exporter.settings.Logger.Error("cannnot start event stream", zap.Error(err))
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
		ae.exporter.settings.Logger.Error("arrow recv", zap.Error(err))
	}

	// Wait for the writer to ensure that all waiters are known.
	ww.Wait()

	// The reader and writer have both finished; respond to any
	// outstanding waiters.
	for _, ch := range stream.waiters {
		ch <- status.Error(codes.Aborted, "stream is restarting")
	}
}

// getArrowStream returns the first-available stream.  This returns nil
// when the consumer should fall back to standard OTLP.
func (e *exporter) getArrowStream(ctx context.Context) (*arrowStream, error) {
	if e.arrow == nil {
		return nil, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case stream := <-e.arrow.ready.readyChannel():
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

writeLoop:
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
			// Because we did not <-stream.toWrite, there is a potential
			// sender race happening, which is handled below.
			break writeLoop
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
			ae.exporter.settings.Logger.Error("arrow encode", zap.Error(err))
			return
		}

		stream.setBatchChannel(batch.BatchId, wri.errCh)

		if err := stream.client.Send(batch); err != nil {
			// The error will be sent to errCh during cleanup for this stream.
			ae.exporter.settings.Logger.Error("arrow send", zap.Error(err))
			return
		}
	}

	// Remove this from the ready channel.
	ae.ready.removeReady(stream)
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

		ae.processBatchStatus(stream, resp.Statuses)
	}
}

// processBatchStatus processes a single response from the server.
func (ae *arrowExporter) processBatchStatus(stream *arrowStream, statuses []*arrowpb.StatusMessage) {
	stream.lock.Lock()
	defer stream.lock.Unlock()

	for _, status := range statuses {
		ch, ok := stream.waiters[status.BatchId]
		if !ok {

			continue
		}

		ch <- nil
		delete(stream.waiters, status.BatchId)
	}
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
func (stream *arrowStream) encode(records interface{}) (*arrowpb.BatchEvent, error) {
	// Note!! This is a placeholder.  After this PR merges the
	// code base will be upgraded to the latest version of
	// github.com/f5/otel-arrow-adapter, which returns one
	// BatchEvent per pdata Logs/Metrics/Traces.  The TODO below
	// will be addressed, i.e., we will not drop all but one
	// batch.
	var batches []*arrowpb.BatchEvent
	var err error
	switch data := records.(type) {
	case ptrace.Traces:
		batches, err = stream.producer.BatchEventsFrom(data)
	case plog.Logs:
		// TODO: WIP in https://github.com/f5/otel-arrow-adapter/pull/13
	case pmetric.Metrics:
		// TODO: This will follow.
	}
	if err != nil {
		return nil, err
	}

	// TODO: DO NOT DROP DATA batches[1..N], see note above.
	batch := batches[0]
	return batch, nil
}

// newPrioritizer constructs a channel-based first-available prioritizer.
func (e *exporter) newPrioritizer() streamPrioritizer {
	return streamPrioritizer{
		make(chan *arrowStream, e.config.Arrow.NumStreams),
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
			// A consumer got us first.
			wri.errCh <- status.Error(codes.Aborted, "stream is restarting")
		}
	}
}
