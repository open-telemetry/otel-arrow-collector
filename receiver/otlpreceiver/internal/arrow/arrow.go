// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrow

import (
	"context"
	"fmt"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	batchEvent "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/obsreport"
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

		// Process records:
		err = r.processRecords(ctx, req)
		if err != nil {
			return err
		}

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

		err = serverStream.Send(resp)
		if err != nil {
			return err
		}
	}
}

func (r *Receiver) processRecords(ctx context.Context, records *arrowpb.BatchArrowRecords) error {
	payloads := records.GetOtlpArrowPayloads()
	if len(payloads) == 0 {
		return nil
	}
	// TODO: Use the obsreport object to instrument (somehow)
	switch payloads[0].Type {
	case arrowpb.OtlpArrowPayloadType_METRICS:
		// otlp, err := r.arrowConsumer.MetricsFrom(records)
		// if err != nil {
		// 	return err
		// }
		// for _, logs := range otlp {
		// 	err = r.Metrics().ConsumeMetrics(ctx, logs)
		// 	if err != nil {
		// 		return err
		// 	}
		// }
		return ErrNoMetricsConsumer

	case arrowpb.OtlpArrowPayloadType_LOGS:
		otlp, err := r.arrowConsumer.LogsFrom(records)
		if err != nil {
			return err
		}

		for _, logs := range otlp {
			err = r.Logs().ConsumeLogs(ctx, logs)
			if err != nil {
				return err
			}
		}

	case arrowpb.OtlpArrowPayloadType_SPANS:
		otlp, err := r.arrowConsumer.TracesFrom(records)
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
	return nil
}
