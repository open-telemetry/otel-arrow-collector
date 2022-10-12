package arrow

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/obsreport"
	"go.opentelemetry.io/collector/pdata/ptrace"

	arrowpb "github.com/lquerel/otel-arrow-adapter/api/collector/arrow/v1"
	batchEvent "github.com/lquerel/otel-arrow-adapter/pkg/otel/batch_event"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/traces"
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
	arrowpb.UnimplementedEventsServiceServer

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

func (r *Receiver) EventsStream(serverStream arrowpb.EventsService_EventStreamServer) error {
	ctx := serverStream.Context()

	var traceProducer *traces.OtlpProducer

	if r.Traces() != nil {
		traceProducer = traces.NewOtlpProducer()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		}

		resp := &arrowpb.BatchStatus{}

		// Note: How often to send responses?  We can loop here.
		req, err := serverStream.Recv()
		if err != nil {
			return err
		}

		messages, err := r.arrowConsumer.Consume(req)

		status := &arrowpb.StatusMessage{
			BatchId: req.GetBatchId(),
		}
		if err == nil {
			// TODO: error handling is not great here, figure this out.
			for _, msg := range messages {
				switch msg.PayloadType() {
				case arrowpb.OtlpArrowPayloadType_METRICS:
					err = fmt.Errorf("no metrics consumer")
				case arrowpb.OtlpArrowPayloadType_LOGS:
					err = fmt.Errorf("no logs consumer")
				case arrowpb.OtlpArrowPayloadType_SPANS:
					if traceProducer == nil {
						err = fmt.Errorf("no spans consumer")
						continue
					}
					var otlp []ptrace.Traces
					if otlp, err = traceProducer.ProduceFrom(msg.Record()); err == nil {
						for _, batch := range otlp {
							err = r.Traces().ConsumeTraces(ctx, batch)
						}
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
