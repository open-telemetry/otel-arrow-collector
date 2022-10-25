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
)

type arrowExporter struct {
	// exporter allows this to refer to all the settings
	exporter *exporter
	client   arrowpb.EventsServiceClient

	// streams contains room for NumConsumers streams.
	streams chan *arrowStream

	returning chan *arrowStream

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type writeItem struct {
	records interface{}
	errCh   chan error
}

type arrowStream struct {
	client arrowpb.EventsService_EventStreamClient

	writer chan writeItem

	// producer is exclusive to the holder of the stream
	producer *batchEvent.Producer

	// cancel cancels stream context
	cancel context.CancelFunc

	// lock protects waiters
	lock    sync.Mutex
	waiters map[string]chan error
}

func (e *exporter) startArrowExporter() *arrowExporter {
	bgctx, bgcancel := context.WithCancel(context.Background())
	bgctx = e.enhanceContext(bgctx)

	ae := &arrowExporter{
		exporter:  e,
		client:    arrowpb.NewEventsServiceClient(e.clientConn),
		streams:   make(chan *arrowStream, e.config.NumConsumers),
		returning: make(chan *arrowStream, e.config.NumConsumers),
		cancel:    bgcancel,
	}
	ae.wg.Add(1)
	go ae.runStreamController(bgctx)

	return ae
}

func (ae *arrowExporter) runStreamController(bgctx context.Context) {
	defer ae.wg.Done()

	for i := 0; i < cap(ae.streams); i++ {
		ae.wg.Add(1)
		go ae.startArrowStream(bgctx)
	}

	for {
		// @@@
		select {
		case stream := <-ae.returning:

			if stream.client == nil {

			}

		case <-bgctx.Done():
			return
		}
	}
}

func (ae *arrowExporter) startArrowStream(bgctx context.Context) {
	defer ae.wg.Done()

	ctx, cancel := context.WithCancel(bgctx)

	stream := &arrowStream{
		writer:   make(chan writeItem, 1),
		producer: batchEvent.NewProducer(),
		waiters:  map[string]chan error{},
		cancel:   cancel,
	}

	if sc, err := ae.client.EventStream(ctx, ae.exporter.callOptions...); err != nil {
		// TODO: if this is a "no such method" error,
		// downgrade to Standard OTLP w/ close(ae.streams).
		// For now always downgrade:
		ae.streams <- nil
		ae.exporter.settings.Logger.Error("cannnot start event stream", zap.Error(err))
	} else {
		// Setting .client != nil indicates that the endpoint
		stream.client = sc
		ae.streams <- stream

		ae.wg.Add(1)

		// TODO: somewhere here, wait for reader/writer then return to all?
		go ae.writeStream(ctx, stream)

		ae.readStream(ctx, stream)
	}

	ae.returning <- stream
}

func (e *exporter) getArrowStream() *arrowStream {
	if e.arrow == nil {
		return nil
	}
	for {
		stream := <-ae.streams
		if stream == nil {
			return nil
		}
		return stream
	}
}

func (ae *arrowExporter) shutdown(ctx context.Context) error {
	ae.cancel()
	ae.wg.Wait()
	// @@@ hmm?
	return nil
}

func (stream *arrowStream) setBatchChannel(batchID string, errCh chan error) {
	stream.lock.Lock()
	defer stream.lock.Unlock()

	stream.waiters[batchID] = errCh
}

func (ae *arrowExporter) writeStream(ctx context.Context, stream *arrowStream) {
	defer stream.cancel()

	for {
		wri := <-stream.writer

		batch, err := stream.encode(wri.records)
		if err != nil {
			wri.errCh <- err
			ae.exporter.settings.Logger.Error("arrow encode", zap.Error(err))
			// TODO: Should we continue--what caused the
			// encode failure?  Returning here breaks the
			// stream.  If individual encode errors are
			// tolerated, do not return.
			return
		}

		stream.setBatchChannel(batch.BatchId, wri.errCh)

		if err := stream.client.Send(batch); err != nil {
			ae.exporter.settings.Logger.Error("arrow send", zap.Error(err))
			return
		}

		ae.streams <- stream
	}
}

func (ae *arrowExporter) readStream(ctx context.Context, stream *arrowStream) {
	defer stream.cancel()

	for {
		resp, err := stream.client.Recv()

		if err != nil {
			ae.exporter.settings.Logger.Error("arrow recv", zap.Error(err))
			return
		}

		ae.processBatchStatus(stream, resp.Statuses)
	}
}

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

func (ae *arrowExporter) sendAndWait(ctx context.Context, stream *arrowStream, records interface{}) error {
	errCh := make(chan error, 1)
	stream.writer <- writeItem{
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
