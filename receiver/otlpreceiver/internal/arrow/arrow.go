package arrow

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/obsreport"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	batchEvent "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record"
	"github.com/f5/otel-arrow-adapter/pkg/otel/traces"
)

const (
	receiverTransport = "otlp-arrow"
)

var (
	ErrNoMetricsConsumer   = fmt.Errorf("no metrics consumer")
	ErrNoLogsConsumer      = fmt.Errorf("no logs consumer")
	ErrNoTracesConsumer    = fmt.Errorf("no traces consumer")
	ErrUnrecognizedPayload = fmt.Errorf("unrecognized OTLP payload")
)

type Consumers interface {
	Traces() consumer.Traces
	Metrics() consumer.Metrics
	Logs() consumer.Logs
}

type Receiver struct {
	Consumers
	arrowpb.UnimplementedArrowStreamServiceServer

	obsrecv       *obsreport.Receiver
	arrowConsumer *batchEvent.Consumer
}

type producers struct {
	Traces *traces.OtlpProducer
	// TODO: Logs
	// TODO: Metrics
}

// New creates a new Receiver reference.
func New(
	id config.ComponentID,
	cs Consumers,
	set component.ReceiverCreateSettings,
) *Receiver {
	obs := obsreport.NewReceiver(obsreport.ReceiverSettings{
		ReceiverID:             id,
		Transport:              receiverTransport,
		ReceiverCreateSettings: set,
	})
	return &Receiver{
		Consumers:     cs,
		obsrecv:       obs,
		arrowConsumer: batchEvent.NewConsumer(),
	}
}

func (r *Receiver) ArrowStream(serverStream arrowpb.ArrowStreamService_ArrowStreamServer) error {
	ctx := serverStream.Context()
	producers := r.newProducers()

	for {
		// See if the context has been canceled.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Receive a batch:
		req, err := serverStream.Recv()
		if err != nil {
			return err
		}

		// Convert to records:
		records, err := r.arrowConsumer.Consume(req)
		if err != nil {
			return err
		}
		// Process records:
		err = r.processRecords(ctx, records, producers)
		if err != nil {
			return err
		}

		fmt.Println("WE DID IT")

		// TODO: We are not required to return a Status per
		// request, what should the logic be?  For now sending
		// one status per request received:
		resp := &arrowpb.BatchStatus{}
		status := &arrowpb.StatusMessage{
			BatchId:    req.GetBatchId(),
			StatusCode: arrowpb.StatusCode_OK,
			// TODO: `StatusMessage` has some provisions
			// for returning information other than OK w/o
			// breaking the stream, and I am not sure what
			// those conditions are (e.g., retry suggestions).
		}
		resp.Statuses = append(resp.Statuses, status)

		serverStream.Send(resp)
	}
}

func (r *Receiver) processRecords(ctx context.Context, records []*batchEvent.RecordMessage, producers producers) error {
	for _, msg := range records {
		switch msg.PayloadType() {
		case arrowpb.OtlpArrowPayloadType_METRICS:
			return ErrNoMetricsConsumer
		case arrowpb.OtlpArrowPayloadType_LOGS:
			return ErrNoLogsConsumer
		case arrowpb.OtlpArrowPayloadType_SPANS:
			if producers.Traces == nil {
				return ErrNoTracesConsumer
			}
			// TODO: Use the obsreport object to instrument (somehow)
			otlp, err := producers.Traces.ProduceFrom(msg.Record())
			if err != nil {
				return err
			}
			for _, traces := range otlp {
				err = r.Traces().ConsumeTraces(ctx, traces)
				if err != nil {
					return err
				}
			}

		default:
			return ErrUnrecognizedPayload
		}
	}
	return nil
}

func (r *Receiver) newProducers() (p producers) {
	if r.Traces() != nil {
		p.Traces = traces.NewOtlpProducer()
	}
	// TODO: Logs
	// TODO: Metrics
	return
}
