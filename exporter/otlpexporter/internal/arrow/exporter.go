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
	"sync"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	arrowRecord "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/collector/component"
)

// High-level TODOs:
// TODO: Use the MAX_CONNECTION_AGE and MAX_CONNECTION_AGE_GRACE settings.

// Exporter is 1:1 with exporter, isolates arrow-specific
// functionality.
type Exporter struct {
	// settings contains Arrow-specific parameters.
	settings Settings

	// telemetry includes logger, tracer, meter.
	telemetry component.TelemetrySettings

	// client uses the exporter's gRPC ClientConn (or is a mock, in tests).
	client arrowpb.ArrowStreamServiceClient

	// grpcOptions includes options used by the unary RPC methods,
	// e.g., WaitForReady.
	grpcOptions []grpc.CallOption

	// ready prioritizes streams that are ready to send
	ready *streamPrioritizer

	// returning is used to pass broken, gracefully-terminated,
	// and otherwise to the stream controller.
	returning chan *Stream

	// cancel cancels the background context of this
	// Exporter, used for shutdown.
	cancel context.CancelFunc

	// wg counts one per active goroutine belonging to all strings
	// of this exporter.  The wait group has Add(1) called before
	// starting goroutines so that they can be properly waited for
	// in shutdown(), so the pattern is:
	//
	//   wg.Add(1)
	//   go func() {
	//     defer wg.Done()
	//     ...
	//   }()
	wg sync.WaitGroup
}

// NewExporter configures a new Exporter.
func NewExporter(
	settings Settings,
	telemetry component.TelemetrySettings,
	client arrowpb.ArrowStreamServiceClient,
	grpcOptions []grpc.CallOption,
) *Exporter {
	return &Exporter{
		settings:    settings,
		telemetry:   telemetry,
		client:      client,
		grpcOptions: grpcOptions,
		returning:   make(chan *Stream, settings.NumStreams),
		ready:       nil,
		cancel:      nil,
	}
}

// Start creates the background context used by all streams and starts
// a stream controller, which initializes the initial set of streams.
func (e *Exporter) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)

	e.cancel = cancel
	e.wg.Add(1)
	e.ready = newStreamPrioritizer(ctx, e.settings)

	go e.runStreamController(ctx)

	return nil
}

// runStreamController starts the initial set of streams, then waits for streams to
// terminate one at a time and restarts them.  If streams come back with a nil
// client (meaning that OTLP+Arrow was not supported by the endpoint), it will
// not be restarted.
func (e *Exporter) runStreamController(bgctx context.Context) {
	defer e.cancel()
	defer e.wg.Done()

	running := e.settings.NumStreams

	// Start the initial number of streams
	for i := 0; i < running; i++ {
		e.wg.Add(1)
		go e.runArrowStream(bgctx)
	}

	for {
		select {
		case stream := <-e.returning:
			if stream.client != nil {
				// The stream closed or broken.  Restart it.
				e.wg.Add(1)
				go e.runArrowStream(bgctx)
				continue
			}
			// Otherwise, the stream never got started.  It was
			// downgraded and senders will use the standard OTLP path.
			running--

			// None of the streams were able to connect to
			// an Arrow endpoint.
			if running == 0 {
				e.telemetry.Logger.Info("failed to establish OTLP+Arrow streaming, downgrading")
				e.ready.downgrade()
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
func (e *Exporter) runArrowStream(bgctx context.Context) {
	ctx, cancel := context.WithCancel(bgctx)

	stream := &Stream{
		toWrite:  make(chan writeItem, 1),
		producer: arrowRecord.NewProducer(),
		waiters:  map[string]chan error{},
		cancel:   cancel,
	}

	defer func() {
		e.wg.Done()
		cancel()
		e.returning <- stream
	}()

	sc, err := e.client.ArrowStream(ctx, e.grpcOptions...)
	if err != nil {
		// TODO: only when this is a permanent (e.g., "no such
		// method") error, downgrade to standard OTLP.
		// Returning with stream.client == nil signals the
		// lack of an Arrow stream endpoint.  When all the
		// streams return with .client == nil, the ready
		// channel will be closed.
		e.telemetry.Logger.Error("cannot start event stream", zap.Error(err))
		return
	}
	// Setting .client != nil indicates that the endpoint was valid,
	// streaming may start.  When this stream finishes, it will be
	// restarted.
	stream.client = sc

	// ww is used to wait for the writer.
	var ww sync.WaitGroup

	ww.Add(1)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		stream.write(ctx, e, &ww)
	}()

	if err := stream.read(ctx); err != nil {
		// TODO: should this log even an io.EOF error?
		e.telemetry.Logger.Error("arrow recv", zap.Error(err))
	}

	// Wait for the writer to ensure that all waiters are known.
	ww.Wait()

	// The reader and writer have both finished; respond to any
	// outstanding waiters.
	for _, ch := range stream.waiters {
		// Note: exporterhelper will retry.
		// TODO: Would it be better to handle retry in this directly?
		ch <- status.Error(codes.Aborted, "stream is restarting")
	}
}

// GetStream is called to get an available stream with the user's pipeline context.
func (e *Exporter) GetStream(ctx context.Context) (*Stream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case stream := <-e.ready.readyChannel():
		return stream, nil
	}
}

// Shutdown returns when all Arrow-associated goroutines have returned.
func (e *Exporter) Shutdown(ctx context.Context) error {
	e.cancel()
	e.wg.Wait()
	return nil
}
