package arrow

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Note: there is no provision here or in the package up two levels to
// share an arrow exporter by more than one export pipeline.  This is
// possible--done using static variables in parts of the contrib
// collector (see the internal SharedComponent).

type ArrowExporter struct {
	cset component.ExporterCreateSettings
	cfg  config.Exporter
	eset config.ExporterSettings
	tset exporterhelper.TimeoutSettings
	rset exporterhelper.RetrySettings
	qset exporterhelper.QueueSettings
	gset configgrpc.GRPCClientSettings
}

type TracesExporterFactory func(
	context.Context,
	component.ExporterCreateSettings,
	config.Exporter,
) (component.TracesExporter, error)

type ArrowTracesExporter struct {
	*ArrowExporter
	fallback  TracesExporterFactory
	downgrade component.TracesExporter
}

type MetricsExporterFactory func(
	context.Context,
	component.ExporterCreateSettings,
	config.Exporter,
) (component.MetricsExporter, error)

type ArrowMetricsExporter struct {
	*ArrowExporter
	fallback MetricsExporterFactory
}

type LogsExporterFactory func(
	context.Context,
	component.ExporterCreateSettings,
	config.Exporter,
) (component.LogsExporter, error)

type ArrowLogsExporter struct {
	*ArrowExporter
	fallback LogsExporterFactory
}

func newExporter(
	_ context.Context,
	cset component.ExporterCreateSettings,
	cfg config.Exporter,
	eset config.ExporterSettings,
	tset exporterhelper.TimeoutSettings,
	rset exporterhelper.RetrySettings,
	qset exporterhelper.QueueSettings,
	gset configgrpc.GRPCClientSettings,
) *ArrowExporter {
	return &ArrowExporter{
		cset: cset,
		cfg:  cfg,
		eset: eset,
		tset: tset,
		rset: rset,
		qset: qset,
		gset: gset,
	}
}

func NewTracesExporter(
	ctx context.Context,
	cset component.ExporterCreateSettings,
	cfg config.Exporter,
	eset config.ExporterSettings,
	tset exporterhelper.TimeoutSettings,
	rset exporterhelper.RetrySettings,
	qset exporterhelper.QueueSettings,
	gset configgrpc.GRPCClientSettings,
	fallback TracesExporterFactory,
) (component.TracesExporter, error) {
	return &ArrowTracesExporter{
		ArrowExporter: newExporter(ctx, cset, cfg, eset, tset, rset, qset, gset),
		fallback:      fallback,
	}, nil
}

func NewMetricsExporter(
	ctx context.Context,
	cset component.ExporterCreateSettings,
	cfg config.Exporter,
	eset config.ExporterSettings,
	tset exporterhelper.TimeoutSettings,
	rset exporterhelper.RetrySettings,
	qset exporterhelper.QueueSettings,
	gset configgrpc.GRPCClientSettings,
	fallback MetricsExporterFactory,
) (component.MetricsExporter, error) {
	return &ArrowMetricsExporter{
		ArrowExporter: newExporter(ctx, cset, cfg, eset, tset, rset, qset, gset),
		fallback:      fallback,
	}, nil
}

func NewLogsExporter(
	ctx context.Context,
	cset component.ExporterCreateSettings,
	cfg config.Exporter,
	eset config.ExporterSettings,
	tset exporterhelper.TimeoutSettings,
	rset exporterhelper.RetrySettings,
	qset exporterhelper.QueueSettings,
	gset configgrpc.GRPCClientSettings,
	fallback LogsExporterFactory,
) (component.LogsExporter, error) {
	return &ArrowLogsExporter{
		ArrowExporter: newExporter(ctx, cset, cfg, eset, tset, rset, qset, gset),
		fallback:      fallback,
	}, nil
}

func (ae *ArrowExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: false,
	}
}

func (ae *ArrowExporter) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (ae *ArrowExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (at *ArrowTracesExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return nil
}

func (at *ArrowMetricsExporter) ConsumeMetrics(ctx context.Context, td pmetric.Metrics) error {
	return nil
}

func (at *ArrowLogsExporter) ConsumeLogs(ctx context.Context, td plog.Logs) error {
	return nil
}
