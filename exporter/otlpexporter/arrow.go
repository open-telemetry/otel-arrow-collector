package otlpexporter

import (
	"context"
	"sync"
	"sync/atomic"

	arrowpb "github.com/lquerel/otel-arrow-adapter/api/collector/arrow/v1"
	batchEvent "github.com/lquerel/otel-arrow-adapter/pkg/otel/batch_event"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type arrowStream struct {
	client arrowpb.EventsService_EventStreamClient

	lock     sync.Mutex
	producer *batchEvent.Producer
	waiters  map[string]chan error
}

type arrowExporter struct {
	*exporter

	client  arrowpb.EventsServiceClient
	streams []*arrowStream // size = NumConsumers
	rr      atomic.Int64   // round-robin for stream choice

	stop func() // cancels context used by consumers
	wg   sync.WaitGroup
}

type arrowTracesExporter struct {
	*arrowExporter
}

type arrowMetricsExporter struct {
	*arrowExporter
}

type arrowLogsExporter struct {
	*arrowExporter
}

func (e *exporter) newArrowExporter(_ context.Context) *arrowExporter {
	return &arrowExporter{
		exporter: e,
	}
}

func (e *exporter) newArrowTracesExporter(ctx context.Context) (*arrowTracesExporter, error) {
	return &arrowTracesExporter{
		arrowExporter: e.newArrowExporter(ctx),
	}, nil
}

func (e *exporter) newArrowMetricsExporter(ctx context.Context) (*arrowMetricsExporter, error) {
	// exp, err := e.createStandardMetricsExporter(ctx, nil)
	// if err != nil {
	// 	return nil, err
	// }
	return &arrowMetricsExporter{
		arrowExporter: e.newArrowExporter(ctx),
	}, nil
}

func (e *exporter) newArrowLogsExporter(ctx context.Context) (*arrowLogsExporter, error) {
	// exp, err := e.createStandardLogsExporter(ctx, nil)
	// if err != nil {
	// 	return nil, err
	// }
	return &arrowLogsExporter{
		arrowExporter: e.newArrowExporter(ctx),
	}, nil
}

func (ae *arrowExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: false,
	}
}

func newArrowStream(sc arrowpb.EventsService_EventStreamClient) *arrowStream {
	return &arrowStream{
		client:   sc,
		producer: batchEvent.NewProducer(),
	}
}

func (ae *arrowExporter) Start(ctx context.Context, h component.Host) error {
	err := ae.exporter.start(ctx, h)
	if err != nil {
		return err
	}

	bgctx, cancel := context.WithCancel(context.Background())
	bgctx = ae.enhanceContext(bgctx)

	ae.client = arrowpb.NewEventsServiceClient(ae.clientConn)
	ae.stop = cancel

	for i := 0; i < ae.config.NumConsumers; i++ {
		sc, err := ae.client.EventStream(bgctx, ae.callOptions...)
		if err != nil {
			return err
		}
		stream := newArrowStream(sc)

		go stream.run(ctx, ae)

		ae.streams = append(ae.streams, stream)
		ae.wg.Add(1)
	}

	return nil
}

func (stream *arrowStream) run(ctx context.Context, ae *arrowExporter) {
	defer ae.wg.Done()

}

func (ae *arrowExporter) Shutdown(ctx context.Context) error {
	ae.stop()
	ae.wg.Wait()
	return ae.exporter.shutdown(ctx)
}

func (ae *arrowTracesExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return ae.consumeAndWait(ctx, td)
}

func (ae *arrowMetricsExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return ae.consumeAndWait(ctx, md)
}

func (ae *arrowLogsExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return ae.consumeAndWait(ctx, ld)
}

func (ae *arrowExporter) pickStream() *arrowStream {
	return ae.streams[ae.rr.Add(1)%int64(ae.config.NumConsumers)]
}

func (ae *arrowExporter) consumeAndWait(ctx context.Context, records interface{}) error {
	cleanup, ch, err := ae.consume(ctx, records)
	if err != nil {
		return err
	}
	defer cleanup()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ch:
		return err
	}
}

func (ae *arrowExporter) consume(ctx context.Context, records interface{}) (func(), chan error, error) {
	stream := ae.pickStream()
	stream.lock.Lock()
	defer stream.lock.Unlock()

	// Note: Discussed w/ LQ 10/20/22 this producer method will
	// change to produce one batch event per input, i.e., Send()
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
		return nil, nil, err
	}
	// TODO: DO NOT DROP DATA batches[1..N]
	batch := batches[0]

	ch := make(chan error)
	stream.waiters[batch.BatchId] = ch

	if err := stream.client.Send(batch); err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		close(ch)

		stream.lock.Lock()
		defer stream.lock.Unlock()

		delete(stream.waiters, batch.BatchId)
	}
	return cleanup, ch, nil
}
