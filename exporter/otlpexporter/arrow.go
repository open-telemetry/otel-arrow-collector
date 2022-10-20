package otlpexporter

import (
	"context"

	"github.com/apache/arrow/go/v9/arrow"
	arrowpb "github.com/lquerel/otel-arrow-adapter/api/collector/arrow/v1"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/traces"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type arrowExporter struct {
	*exporter

	arrowClient arrowpb.EventsServiceClient
}

type arrowTracesExporter struct {
	*arrowExporter
	producer *traces.OtlpArrowProducer
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
		producer:      traces.NewOtlpArrowProducer(),
		// TODO: standard exporter fallback
	}, nil
}

func (e *exporter) newArrowMetricsExporter(ctx context.Context) (*arrowMetricsExporter, error) {
	exp, err := e.createStandardMetricsExporter(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &arrowMetricsExporter{
		arrowExporter: e.newArrowExporter(ctx),
		// TODO: arrow producer
		standardExporter: exp,
	}, nil
}

func (e *exporter) newArrowLogsExporter(ctx context.Context) (*arrowLogsExporter, error) {
	exp, err := e.createStandardLogsExporter(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &arrowLogsExporter{
		arrowExporter: e.newArrowExporter(ctx),
		// TODO: arrow producer
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

	return nil
}

func (ae *arrowExporter) Shutdown(ctx context.Context) error {
	return ae.exporter.shutdown(ctx)
}

func (ae *arrowTracesExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	recs, err := ae.producer.ProduceFrom(td)
	if err != nil {
		return err
	}
	// TODO: somewhere here, handle a downgrade to the standard exporter.
	return ae.consume(recs)
}

func (ae *arrowMetricsExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return ae.standardExporter.ConsumeMetrics(ctx, md)
}

func (ae *arrowLogsExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return ae.standardExporter.ConsumeLogs(ctx, ld)
}

func (ae *arrowExporter) consume(recs []arrow.Record) error {
	// @@@
	return nil
}
