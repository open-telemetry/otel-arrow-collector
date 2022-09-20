package arrow

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/obsreport"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	arrowpb "github.com/lquerel/otel-arrow-adapter/api/collector/arrow/v1"
	batchEvent "github.com/lquerel/otel-arrow-adapter/pkg/otel/batch_event"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/logs"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/metrics"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/trace"
)

const (
	dataFormatArrow   = "arrow"
	receiverTransport = "grpc"
)

type Consumers interface {
	Traces() consumer.Traces
	Metrics() consumer.Metrics
	Logs() consumer.Logs
}

type Receiver struct {
	Consumers
	arrowpb.UnimplementedArrowServiceServer

	obsrecv       *obsreport.Receiver
	arrowConsumer *batchEvent.Consumer
}

// New creates a new Receiver reference.
func New(
	id config.ComponentID,
	cs Consumers,
	set component.ReceiverCreateSettings,
) *Receiver {
	return &Receiver{
		Consumers: cs,
		obsrecv: obsreport.NewReceiver(obsreport.ReceiverSettings{
			ReceiverID:             id,
			Transport:              receiverTransport,
			ReceiverCreateSettings: set,
		}),
		arrowConsumer: batchEvent.NewConsumer(),
	}
}

func (r *Receiver) ArrowStream(serverStream arrowpb.ArrowService_ArrowStreamServer) error {
	ctx := serverStream.Context()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		}

		resp := &arrowpb.ArrowStatus{}

		// Note: How often to send responses?  We can loop here.
		req, err := serverStream.Recv()
		if err != nil {
			return err
		}

		messages, err := r.arrowConsumer.Consume(req)

		status := &arrowpb.StatusMessage{
			BatchId: req.GetBatchId(),
		}
		if err != nil {
			// TODO: error handling is not great here, figure this out.
			for _, msg := range messages {
				switch msg.PayloadType() {
				case arrowpb.PayloadType_METRICS:
					if r.Metrics() == nil {
						// TODO: set ErrorCode not retryable, e.g.,
						err = fmt.Errorf("no metrics consumer")
						continue
					}
					var otlp pmetric.Metrics
					if otlp, err = metrics.ArrowRecordsToOtlpMetrics(msg.Record()); err == nil {
						err = r.Metrics().ConsumeMetrics(ctx, otlp)
					}

				case arrowpb.PayloadType_LOGS:
					if r.Logs() == nil {
						err = fmt.Errorf("no logs consumer")
						continue
					}
					var otlp plog.Logs
					if otlp, err = logs.ArrowRecordsToOtlpLogs(msg.Record()); err == nil {
						err = r.Logs().ConsumeLogs(ctx, otlp)
					}

				case arrowpb.PayloadType_SPANS:
					if r.Traces() == nil {
						err = fmt.Errorf("no spans consumer")
						continue
					}
					var otlp ptrace.Traces
					if otlp, err = trace.ArrowRecordsToOtlpTrace(msg.Record()); err == nil {
						err = r.Traces().ConsumeTraces(ctx, otlp)
					}

				default:
					err = fmt.Errorf("unrecognized OTLP payload type")
				}
			}
		}
		if err == nil {
			status.StatusCode = arrowpb.StatusCode_OK
		} else {
			status.StatusCode = arrowpb.StatusCode_ERROR
			status.ErrorCode = 0 // TODO: ErrorCode?
			status.ErrorMessage = err.Error()
			status.RetryInfo = nil // TODO: RetryInfo?
		}

		resp.Statuses = append(resp.Statuses, status)

		serverStream.Send(resp)
	}
}
