package otlpexporter

import (
	"context"
	"sync"

	arrowpb "github.com/lquerel/otel-arrow-adapter/api/collector/arrow/v1"
	batchEvent "github.com/lquerel/otel-arrow-adapter/pkg/otel/batch_event"
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

	// streams contains room for NumConsumers streams; senders
	// will take the next available stream.  When an OTLP+Arrow
	// stream downgrades to standard OTLP nil values are used.
	streams chan *arrowStream

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
		streams:   make(chan *arrowStream, e.config.Arrow.NumStreams),
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

	// Start the initial number of streams
	for i := 0; i < ae.exporter.config.Arrow.NumStreams; i++ {
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
			} else {
				// Otherwise, the stream never got started.  It was
				// downgraded and senders will use the standard OTLP
				// path as this exporter is alive--a nil *arrowStream
				// is being used.
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
	defer ae.wg.Done()

	ctx, cancel := context.WithCancel(bgctx)

	defer cancel()

	stream := &arrowStream{
		toWrite:  make(chan writeItem, 1),
		producer: batchEvent.NewProducer(),
		waiters:  map[string]chan error{},
		cancel:   cancel,
	}

	if sc, err := ae.client.EventStream(ctx, ae.exporter.callOptions...); err != nil {
		// TODO: only when this is a "no such method" error, downgrade to
		// standard OTLP by placing a nil stream into the streams channel.
		// For now always downgrade:
		ae.streams <- nil
		ae.exporter.settings.Logger.Error("cannnot start event stream", zap.Error(err))
	} else {
		// Setting .client != nil indicates that the endpoint was valid,
		// streaming may start.  When this stream finishes, it will be
		// restarted.
		stream.client = sc

		var ww sync.WaitGroup

		ww.Add(1)
		ae.wg.Add(1)
		go ae.writeStream(ctx, stream, &ww)

		ae.readStream(ctx, stream)

		// Wait for the writer to ensure that all waiters are released
		// below.
		ww.Wait()

		// The reader and writer have both finished; respond to any
		// outstanding waiters.
		for _, ch := range stream.waiters {
			ch <- status.Error(codes.Aborted, "stream is restarting")
		}
	}

	// The stream is finished, return to the stream controller.
	ae.returning <- stream
}

// getArrowStream returns the first-available stream.  This returns nil
// when the consumer should fall back to standard OTLP.
func (e *exporter) getArrowStream() *arrowStream {
	if e.arrow == nil {
		return nil
	}
	stream := <-e.arrow.streams
	if stream == nil {
		return nil
	}
	return stream
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
		ae.streams <- stream

		// this can block, and if the context is canceled we
		// wait for the reader to find this stream.
		var wri writeItem
		select {
		case wri = <-stream.toWrite:
		case <-ctx.Done():
			// Because we did not <-stream.toWrite there is a potential
			// sender race happening, which is handled below.
			break writeLoop
		}
		// Note: For the two return statements below there is no potential
		// sender race because the stream is not available, as indicated by
		// the successful <-stream.toWrite.

		batch, err := stream.encode(wri.records)
		if err != nil {
			// This is some kind of internal error, and the write item
			// has not been put into the waiters struct yet. The stream
			// will be canceled immediately.  TODO how do we know if
			// this should be a permanent error?
			wri.errCh <- err
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

	// For the break statement above, in case a sender is trying to use the stream.
	stream.cancel()
	for i := 0; i < ae.exporter.config.Arrow.NumStreams; i++ {
		var alternate *arrowStream
		select {
		case alternate = <-ae.streams:
			if alternate == stream {
				return
			}
			ae.streams <- alternate
		case wri := <-stream.toWrite:
			// a consumer got us first
			wri.errCh <- status.Error(codes.Aborted, "stream is restarting")
		}
	}
}

// readStream repeatedly reads a batch status and releases the consumers waiting for
// a response.
func (ae *arrowExporter) readStream(ctx context.Context, stream *arrowStream) {
	defer stream.cancel()

	for {
		resp, err := stream.client.Recv()

		if err != nil {
			ae.exporter.settings.Logger.Error("arrow recv", zap.Error(err))
			break
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
	// Note: Discussed w/ LQ 10/20/22 this producer method will
	// change to produce one batch event per input, i.e., one Send()
	// call each.
	var batches []*arrowpb.BatchEvent
	var err error
	switch data := records.(type) {
	case ptrace.Traces:
		batches, err = stream.producer.BatchEventsFrom(data)
	case plog.Logs:
		// TODO
	case pmetric.Metrics:
		// TODO
	}
	if err != nil {
		return nil, err
	}

	// TODO: DO NOT DROP DATA batches[1..N], see note above.
	batch := batches[0]
	return batch, nil
}
