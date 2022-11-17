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
	arrowRecord "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record"

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
	arrowConsumer arrowRecord.ConsumerAPI
}

// New creates a new Receiver reference.
func New(
	id config.ComponentID,
	cs Consumers,
	set component.ReceiverCreateSettings,
	ac arrowRecord.ConsumerAPI,
) *Receiver {
	obs := obsreport.NewReceiver(obsreport.ReceiverSettings{
		ReceiverID:             id,
		Transport:              receiverTransport,
		ReceiverCreateSettings: set,
	})
	return &Receiver{
		Consumers:     cs,
		obsrecv:       obs,
		arrowConsumer: ac,
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

		// Process records: an error in this code path does
		// not necessarily break the stream.
		invalid, err := r.processRecords(ctx, req)

		// Note: Statuses can be batched: TODO: should we?
		resp := &arrowpb.BatchStatus{}
		status := &arrowpb.StatusMessage{
			BatchId: req.GetBatchId(),
		}
		if err == nil {
			status.StatusCode = arrowpb.StatusCode_OK
		} else {
			status.StatusCode = arrowpb.StatusCode_ERROR
			status.ErrorMessage = err.Error()

			if invalid {
				status.ErrorCode = arrowpb.ErrorCode_INVALID_ARGUMENT
			} else {
				status.ErrorCode = arrowpb.ErrorCode_UNAVAILABLE
			}
		}
		resp.Statuses = append(resp.Statuses, status)

		err = serverStream.Send(resp)
		if err != nil {
			return err
		}
	}
}

// processRecords returns an error and a boolean indicating whether
// the error (true) was from processing the data (i.e., invalid
// argument) or (false) from the consuming pipeline.  The boolean is
// not used when success (nil error) is returned.
func (r *Receiver) processRecords(ctx context.Context, records *arrowpb.BatchArrowRecords) (invalid bool, _ error) {
	payloads := records.GetOtlpArrowPayloads()
	if len(payloads) == 0 {
		return false, nil
	}
	// TODO: Use the obsreport object to instrument (somehow)
	switch payloads[0].Type {
	case arrowpb.OtlpArrowPayloadType_METRICS:
		otlp, err := r.arrowConsumer.MetricsFrom(records)
		if err != nil {
			return true, err
		}
		for _, metrics := range otlp {
			err = r.Metrics().ConsumeMetrics(ctx, metrics)
			if err != nil {
				return false, err
			}
		}

	case arrowpb.OtlpArrowPayloadType_LOGS:
		otlp, err := r.arrowConsumer.LogsFrom(records)
		if err != nil {
			return true, err
		}

		for _, logs := range otlp {
			err = r.Logs().ConsumeLogs(ctx, logs)
			if err != nil {
				return false, err
			}
		}

	case arrowpb.OtlpArrowPayloadType_SPANS:
		otlp, err := r.arrowConsumer.TracesFrom(records)
		if err != nil {
			return true, err
		}

		for _, traces := range otlp {
			err = r.Traces().ConsumeTraces(ctx, traces)
			if err != nil {
				return false, err
			}
		}

	default:
		return true, ErrUnrecognizedPayload
	}
	return false, nil
}
