package otlpexporter

import (
	"context"
	"sync"

	arrowpb "github.com/lquerel/otel-arrow-adapter/api/collector/arrow/v1"
	batchEvent "github.com/lquerel/otel-arrow-adapter/pkg/otel/batch_event"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type arrowStream struct {
	client arrowpb.EventsService_EventStreamClient

	// producer is exclusive to the holder of the stream
	producer *batchEvent.Producer

	// cancel cancels stream context
	cancel context.CancelFunc

	// lock protects waiters
	lock    sync.Mutex
	waiters map[string]chan error
}

type arrowExporter struct {
	client arrowpb.EventsServiceClient

	// streams contains room for NumConsumers streams. streams
	// will be closed to downgrade to standard OTLP.
	streams chan *arrowStream

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (e *exporter) startArrowExporter() (*arrowExporter, error) {
	bgctx, bgcancel := context.WithCancel(context.Background())
	bgctx = e.enhanceContext(bgctx)

	ae := &arrowExporter{
		client:  arrowpb.NewEventsServiceClient(e.clientConn),
		streams: make(chan *arrowStream, e.config.NumConsumers),
		cancel:  bgcancel,
	}

	var streams []*arrowStream
	for i := 0; i < e.config.NumConsumers; i++ {
		ctx, cancel := context.WithCancel(bgctx)
		ctx = e.enhanceContext(ctx)

		sc, err := ae.client.EventStream(ctx, e.callOptions...)
		if err != nil {
			// TODO: if this is a "no such method" error, we
			// should downgrade to Standard OTLP.
			return nil, err
		}
		stream := &arrowStream{
			client:   sc,
			producer: batchEvent.NewProducer(),
			waiters:  map[string]chan error{},
			cancel:   cancel,
		}
		streams = append(streams, stream)

		go ae.runStream(ctx, stream)

		ae.wg.Add(1)
		ae.streams <- stream
	}
	return ae, nil
}

func (e *exporter) getArrowStream() *arrowStream {
	if e.arrow == nil {
		return nil
	}
	return e.arrow.pickStream()
}

func (ae *arrowExporter) pickStream() *arrowStream {
	// when ae.streams is closed this returns nil
	return <-ae.streams
}

func (ae *arrowExporter) shutdown(ctx context.Context) error {
	ae.cancel()
	ae.wg.Wait()
	return nil
}

func (ae *arrowExporter) runStream(ctx context.Context, stream *arrowStream) {
	defer stream.cancel()
	defer ae.wg.Done()

	for {
		resp, err := stream.client.Recv()
		// If ...
		if err != nil {

			// probably just log this
			return
		}

		ae.processBatchStatus(stream, resp.Statuses)
	}
}

func (ae *arrowExporter) processBatchStatus(stream *arrowStream, statuses []*arrowpb.StatusMessage) {
	ae.lock.Lock()
	defer ae.lock.Unlock()

	for _, status := range statuses {
		ch, ok := ae.waiters[status.BatchId]
		if ok {
			ch <- nil
		}
	}
}

func (ae *arrowExporter) returnStream(stream *arrowStream) {
	ae.streams <- stream
}

func (ae *arrowExporter) encodeAndSend(ctx context.Context, stream *arrowStream, records interface{}) (string, chan error, error) {
	defer ae.returnStream(stream)

	batch, err := stream.encode(records)
	if err != nil {
		return "", nil, err
	}
	ch, err := stream.send(ctx, batch)
	if err != nil {
		return "", nil, err
	}
	return batch.BatchId, ch, nil
}

func (ae *arrowExporter) sendAndWait(ctx context.Context, stream *arrowStream, records interface{}) error {
	batchID, ch, err := ae.encodeAndSend(ctx, stream, records)
	if err != nil {
		return err
	}

	defer func() {
		close(ch)

		stream.lock.Lock()
		defer stream.lock.Unlock()

		delete(stream.waiters, batchID)
	}()

	select {
	case <-ctx.Done():
		// The caller's pipeline context timed out, but this stream is OK.
		return ctx.Err()
	case err := <-ch:
		return err
	}
}

func (stream *arrowStream) send(_ context.Context, batch *arrowpb.BatchEvent) (chan error, error) {
	// TODO: incoming pipeline context is not used, the Send()
	// will not be canceled because of the incoming
	// context.

	if err := stream.client.Send(batch); err != nil {
		return nil, processGRPCError(err)
	}

	ch := make(chan error)

	stream.lock.Lock()
	defer stream.lock.Unlock()

	stream.waiters[batch.BatchId] = ch

	return ch, nil
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
