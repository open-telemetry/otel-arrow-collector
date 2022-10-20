package otlpexporter

import (
	"context"
	"sync"
	"time"

	arrowpb "github.com/lquerel/otel-arrow-adapter/api/collector/arrow/v1"
	batchEvent "github.com/lquerel/otel-arrow-adapter/pkg/otel/batch_event"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type arrowExporter struct {
	*exporter

	arrowClient arrowpb.EventsServiceClient
	recCh       chan payload
	stop        func() // cancels context used by consumers
	wait        sync.WaitGroup
}

type payload struct {
	records interface{}
	notify  chan error
}

type arrowTracesExporter struct {
	*arrowExporter
}

type arrowMetricsExporter struct {
	*arrowExporter
	// TODO *metrics.OtlpArrowProducer
	standardExporter component.MetricsExporter
}

type arrowLogsExporter struct {
	*arrowExporter
	// TODO *logs.OtlpArrowProducer
	standardExporter component.LogsExporter
}

func (e *exporter) newArrowExporter(_ context.Context) *arrowExporter {
	return &arrowExporter{
		exporter: e,
	}
}

func (e *exporter) newArrowTracesExporter(ctx context.Context) (*arrowTracesExporter, error) {
	return &arrowTracesExporter{
		arrowExporter: e.newArrowExporter(ctx),
		// TODO: standard exporter fallback
	}, nil
}

func (e *exporter) newArrowMetricsExporter(ctx context.Context) (*arrowMetricsExporter, error) {
	exp, err := e.createStandardMetricsExporter(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &arrowMetricsExporter{
		arrowExporter:    e.newArrowExporter(ctx),
		standardExporter: exp,
	}, nil
}

func (e *exporter) newArrowLogsExporter(ctx context.Context) (*arrowLogsExporter, error) {
	exp, err := e.createStandardLogsExporter(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &arrowLogsExporter{
		arrowExporter:    e.newArrowExporter(ctx),
		standardExporter: exp,
	}, nil
}

func (ae *arrowExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: false,
	}
}

func (ae *arrowExporter) Start(ctx context.Context, h component.Host) error {
	err := ae.exporter.start(ctx, h)
	if err != nil {
		return err
	}
	ae.arrowClient = arrowpb.NewEventsServiceClient(ae.clientConn)
	ae.recCh = make(chan payload, ae.config.QueueSettings.QueueSize)
	ctx, cancel := context.WithCancel(context.Background())
	ae.stop = cancel
	ae.wait.Add(1)

	stream, err := ae.arrowClient.EventStream(ae.enhanceContext(ctx), ae.callOptions...)
	if err != nil {
		return err
	}

	go ae.runConsumer(ctx, stream)

	return nil
}

func (ae *arrowExporter) runConsumer(ctx context.Context, stream arrowpb.EventsService_EventStreamClient) {
	// TODO: max delay config?  should be less than ae.config.TimeoutSettings.Timeout
	var ticker = time.NewTicker(time.Second * 10)
	var notifs []chan error
	var notifyAll = func() {
		for _, n := range notifs {
			close(n)
		}
		notifs = notifs[:0]
	}

	defer notifyAll()
	defer ae.wait.Done()
	defer ticker.Stop()

	batchProducer := batchEvent.NewProducer()

	for {
		for { // TODO Max-size test
			select {
			case <-ctx.Done(
				return
			case <-ticker.C:
				break
			case pl := <-ae.recCh:
				var evts []*arrowpb.BatchEvent
				var err error
				switch data := pl.records.(type) {
				case ptrace.Traces:
					evts, err = batchProducer.BatchEventsFrom(data)
				case plog.Logs:
					// TODO
				case pmetric.Metrics:
					// TODO
				}
				if err != nil {
					pl.notify <- err
					continue
				}

				for _, evt := range evts {
					err := stream.Send(evt)
					if err != nil {
						// TODO: retry, etc.
					}
				}

				notifs = append(notifs, pl.notify)
			}
		}

		// notify success:
		notifyAll()
	}
}

func (ae *arrowExporter) Shutdown(ctx context.Context) error {
	ae.stop()
	ae.wait.Wait()
	return ae.exporter.shutdown(ctx)
}

func (ae *arrowTracesExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// TODO: somewhere here, handle a downgrade to the standard exporter.
	return ae.consume(ctx, arrowpb.OtlpArrowPayloadType_SPANS, td)
}

func (ae *arrowMetricsExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// TODO: arrow consume
	return ae.standardExporter.ConsumeMetrics(ctx, md)
}

func (ae *arrowLogsExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	// TODO: arrow consume
	return ae.standardExporter.ConsumeLogs(ctx, ld)
}

func (ae *arrowExporter) consume(ctx context.Context, ptype arrowpb.OtlpArrowPayloadType, records interface{}) error {
	single := make(chan error)

	ae.recCh <- payload{
		records: records,
		notify:  single,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-single:
		return err
	}
}
