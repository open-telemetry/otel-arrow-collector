// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package telemetry // import "go.opentelemetry.io/collector/service/telemetry"

import (
	"context"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Telemetry struct {
	logger         *zap.Logger
	tracerProvider *sdktrace.TracerProvider
}

func (t *Telemetry) TracerProvider() trace.TracerProvider {
	return t.tracerProvider
}

func (t *Telemetry) Logger() *zap.Logger {
	return t.logger
}

func (t *Telemetry) Shutdown(ctx context.Context) error {
	// TODO: Sync logger.
	return multierr.Combine(
		t.tracerProvider.Shutdown(ctx),
	)
}

// Settings holds configuration for building Telemetry.
type Settings struct {
	ZapOptions []zap.Option
}

// New creates a new Telemetry from Config.
func New(_ context.Context, set Settings, cfg Config) (*Telemetry, error) {
	logger, err := newLogger(cfg.Logs, set.ZapOptions)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		// needed for supporting the zpages extension
		sdktrace.WithSampler(alwaysRecord()),
	)
	return &Telemetry{
		logger:         logger,
		tracerProvider: tp,
	}, nil
}

func newLogger(cfg LogsConfig, options []zap.Option) (*zap.Logger, error) {
	// Copied from NewProductionConfig.
	zapCfg := &zap.Config{
		Level:             zap.NewAtomicLevelAt(cfg.Level),
		Development:       cfg.Development,
		Sampling:          toSamplingConfig(cfg.Sampling),
		Encoding:          cfg.Encoding,
		EncoderConfig:     zap.NewProductionEncoderConfig(),
		OutputPaths:       cfg.OutputPaths,
		ErrorOutputPaths:  cfg.ErrorOutputPaths,
		DisableCaller:     cfg.DisableCaller,
		DisableStacktrace: cfg.DisableStacktrace,
		InitialFields:     cfg.InitialFields,
	}

	if zapCfg.Encoding == "console" {
		// Human-readable timestamps for console format of logs.
		zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}

	logger, err := zapCfg.Build(options...)
	if err != nil {
		return nil, err
	}

	return logger, nil
}

func toSamplingConfig(sc *LogsSamplingConfig) *zap.SamplingConfig {
	if sc == nil {
		return nil
	}
	return &zap.SamplingConfig{
		Initial:    sc.Initial,
		Thereafter: sc.Thereafter,
	}
}
