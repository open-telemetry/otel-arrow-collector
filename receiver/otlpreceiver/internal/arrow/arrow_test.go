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
	"errors"
	"fmt"
	"io"
	"testing"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	arrowCollectorMock "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1/mock"
	arrowRecord "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record"
	arrowRecordMock "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/internal/testdata"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver/otlpreceiver/internal/arrow/mock"
)

// Note on protocol buffer comparison:
// EqualValues works for the underlying gogo protos (plog, pmetric, ptrace).
// cmp.Diff with protocmp.Transform option works for current google protobufs.

type commonTestCase struct {
	ctrl      *gomock.Controller
	cancel    context.CancelFunc
	telset    component.TelemetrySettings
	consumers mockConsumers
	stream    *arrowCollectorMock.MockArrowStreamService_ArrowStreamServer
	receive   chan recvResult
	consume   chan interface{}
	streamErr chan error

	testProducer  *arrowRecord.Producer
	arrowConsumer *arrowRecordMock.MockConsumerAPI

	ctxCall  *gomock.Call
	recvCall *gomock.Call
}

type testChannel interface {
	onConsume() error
}

type healthyTestChannel struct{}

func (healthyTestChannel) onConsume() error {
	return nil
}

type unhealthyTestChannel struct{}

func (unhealthyTestChannel) onConsume() error {
	return fmt.Errorf("consumer unhealthy")
}

type recvResult struct {
	payload *arrowpb.BatchArrowRecords
	err     error
}

type mockConsumers struct {
	traces  *mock.MockTraces
	logs    *mock.MockLogs
	metrics *mock.MockMetrics

	tracesCall  *gomock.Call
	logsCall    *gomock.Call
	metricsCall *gomock.Call
}

func newTestTelemetry(t *testing.T) component.TelemetrySettings {
	telset := componenttest.NewNopTelemetrySettings()
	telset.Logger = zaptest.NewLogger(t)
	return telset
}

func (ctc *commonTestCase) putBatch(payload *arrowpb.BatchArrowRecords, err error) {
	ctc.receive <- recvResult{
		payload: payload,
		err:     err,
	}
}

func (ctc *commonTestCase) doAndReturnGetBatch(ctx context.Context) func() (*arrowpb.BatchArrowRecords, error) {
	return func() (*arrowpb.BatchArrowRecords, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case r, ok := <-ctc.receive:
			if !ok {
				return nil, io.EOF
			}
			return r.payload, r.err
		}
	}
}

func (ctc *commonTestCase) doAndReturnConsumeTraces(tc testChannel) func(ctx context.Context, traces ptrace.Traces) error {
	return func(ctx context.Context, traces ptrace.Traces) error {
		select {
		case ctc.consume <- traces:
			return tc.onConsume()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (ctc *commonTestCase) doAndReturnConsumeMetrics(tc testChannel) func(ctx context.Context, metrics pmetric.Metrics) error {
	return func(ctx context.Context, metrics pmetric.Metrics) error {
		select {
		case ctc.consume <- metrics:
			return tc.onConsume()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (ctc *commonTestCase) doAndReturnConsumeLogs(tc testChannel) func(ctx context.Context, logs plog.Logs) error {
	return func(ctx context.Context, logs plog.Logs) error {
		select {
		case ctc.consume <- logs:
			return tc.onConsume()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func newMockConsumers(ctrl *gomock.Controller) mockConsumers {
	mc := mockConsumers{
		traces:  mock.NewMockTraces(ctrl),
		logs:    mock.NewMockLogs(ctrl),
		metrics: mock.NewMockMetrics(ctrl),
	}
	mc.traces.EXPECT().Capabilities().Times(0)
	mc.tracesCall = mc.traces.EXPECT().ConsumeTraces(
		gomock.Any(),
		gomock.Any(),
	).Times(0)
	mc.logs.EXPECT().Capabilities().Times(0)
	mc.logsCall = mc.logs.EXPECT().ConsumeLogs(
		gomock.Any(),
		gomock.Any(),
	).Times(0)
	mc.metrics.EXPECT().Capabilities().Times(0)
	mc.metricsCall = mc.metrics.EXPECT().ConsumeMetrics(
		gomock.Any(),
		gomock.Any(),
	).Times(0)
	return mc
}

func (m mockConsumers) Traces() consumer.Traces {
	return m.traces
}

func (m mockConsumers) Logs() consumer.Logs {
	return m.logs
}
func (m mockConsumers) Metrics() consumer.Metrics {
	return m.metrics
}

var _ Consumers = mockConsumers{}

func newCommonTestCase(t *testing.T, tc testChannel) *commonTestCase {
	ctrl := gomock.NewController(t)
	stream := arrowCollectorMock.NewMockArrowStreamService_ArrowStreamServer(ctrl)
	ac := arrowRecordMock.NewMockConsumerAPI(ctrl)

	ctx, cancel := context.WithCancel(context.Background())

	ctc := &commonTestCase{
		ctrl:          ctrl,
		cancel:        cancel,
		telset:        newTestTelemetry(t),
		consumers:     newMockConsumers(ctrl),
		stream:        stream,
		receive:       make(chan recvResult),
		consume:       make(chan interface{}),
		streamErr:     make(chan error),
		testProducer:  arrowRecord.NewProducer(),
		arrowConsumer: ac,
		ctxCall:       stream.EXPECT().Context().Times(0),
		recvCall:      stream.EXPECT().Recv().Times(0),
	}

	ctc.ctxCall.AnyTimes().Return(ctx)
	ctc.recvCall.AnyTimes().DoAndReturn(ctc.doAndReturnGetBatch(ctx))
	ctc.consumers.tracesCall.AnyTimes().DoAndReturn(ctc.doAndReturnConsumeTraces(tc))
	ctc.consumers.logsCall.AnyTimes().DoAndReturn(ctc.doAndReturnConsumeLogs(tc))
	ctc.consumers.metricsCall.AnyTimes().DoAndReturn(ctc.doAndReturnConsumeMetrics(tc))
	return ctc
}

func (ctc *commonTestCase) cancelAndWait() error {
	ctc.cancel()
	return ctc.wait()
}

func (ctc *commonTestCase) wait() error {
	return <-ctc.streamErr
}

func statusOKFor(batchID string) *arrowpb.BatchStatus {
	return &arrowpb.BatchStatus{
		Statuses: []*arrowpb.StatusMessage{
			{
				BatchId:    batchID,
				StatusCode: arrowpb.StatusCode_OK,
			},
		},
	}
}

func statusUnavailableFor(batchID string, msg string) *arrowpb.BatchStatus {
	return &arrowpb.BatchStatus{
		Statuses: []*arrowpb.StatusMessage{
			{
				BatchId:      batchID,
				StatusCode:   arrowpb.StatusCode_ERROR,
				ErrorCode:    arrowpb.ErrorCode_UNAVAILABLE,
				ErrorMessage: msg,
			},
		},
	}
}

func statusInvalidFor(batchID string, msg string) *arrowpb.BatchStatus {
	return &arrowpb.BatchStatus{
		Statuses: []*arrowpb.StatusMessage{
			{
				BatchId:      batchID,
				StatusCode:   arrowpb.StatusCode_ERROR,
				ErrorCode:    arrowpb.ErrorCode_INVALID_ARGUMENT,
				ErrorMessage: msg,
			},
		},
	}
}

func (ctc *commonTestCase) start() {
	rcvr := New(
		config.NewComponentID("arrowtest"),
		ctc.consumers,
		component.ReceiverCreateSettings{
			TelemetrySettings: ctc.telset,
			BuildInfo:         component.NewDefaultBuildInfo(),
		},
		ctc.arrowConsumer,
	)

	go func() {
		ctc.streamErr <- rcvr.ArrowStream(ctc.stream)
	}()
}

func TestReceiverTraces(t *testing.T) {
	tc := healthyTestChannel{}
	ctc := newCommonTestCase(t, tc)

	td := testdata.GenerateTraces(2)
	batch, err := ctc.testProducer.BatchArrowRecordsFromTraces(td)
	require.NoError(t, err)

	ctc.arrowConsumer.EXPECT().TracesFrom(batch).Times(1).Return([]ptrace.Traces{td}, nil)
	ctc.stream.EXPECT().Send(statusOKFor(batch.BatchId)).Times(1).Return(nil)

	ctc.start()
	ctc.putBatch(batch, nil)

	assert.EqualValues(t, td, <-ctc.consume)

	err = ctc.cancelAndWait()
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
}

func TestReceiverLogs(t *testing.T) {
	tc := healthyTestChannel{}
	ctc := newCommonTestCase(t, tc)

	ld := testdata.GenerateLogs(2)
	batch, err := ctc.testProducer.BatchArrowRecordsFromLogs(ld)
	require.NoError(t, err)

	ctc.arrowConsumer.EXPECT().LogsFrom(batch).Times(1).Return([]plog.Logs{ld}, nil)
	ctc.stream.EXPECT().Send(statusOKFor(batch.BatchId)).Times(1).Return(nil)

	ctc.start()
	ctc.putBatch(batch, nil)

	assert.EqualValues(t, ld, <-ctc.consume)

	err = ctc.cancelAndWait()
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "for %v", err)
}

func TestReceiverMetrics(t *testing.T) {
	tc := healthyTestChannel{}
	ctc := newCommonTestCase(t, tc)

	md := testdata.GenerateMetrics(2)
	batch, err := ctc.testProducer.BatchArrowRecordsFromMetrics(md)
	require.NoError(t, err)

	ctc.arrowConsumer.EXPECT().MetricsFrom(batch).Times(1).Return([]pmetric.Metrics{md}, nil)
	ctc.stream.EXPECT().Send(statusOKFor(batch.BatchId)).Times(1).Return(nil)

	ctc.start()
	ctc.putBatch(batch, nil)

	assert.EqualValues(t, md, <-ctc.consume)

	err = ctc.cancelAndWait()
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "for %v", err)
}

func TestReceiverRecvError(t *testing.T) {
	tc := healthyTestChannel{}
	ctc := newCommonTestCase(t, tc)

	ctc.start()

	ctc.putBatch(nil, fmt.Errorf("test recv error"))

	err := ctc.wait()
	require.Error(t, err)
	require.Contains(t, err.Error(), "test recv error")
}

func TestReceiverSendError(t *testing.T) {
	tc := healthyTestChannel{}
	ctc := newCommonTestCase(t, tc)

	ld := testdata.GenerateLogs(2)
	batch, err := ctc.testProducer.BatchArrowRecordsFromLogs(ld)
	require.NoError(t, err)

	ctc.arrowConsumer.EXPECT().LogsFrom(batch).Times(1).Return([]plog.Logs{ld}, nil)
	ctc.stream.EXPECT().Send(statusOKFor(batch.BatchId)).Times(1).Return(fmt.Errorf("test send error"))

	ctc.start()
	ctc.putBatch(batch, nil)

	assert.EqualValues(t, ld, <-ctc.consume)

	err = ctc.wait()
	require.Error(t, err)
	require.Contains(t, err.Error(), "test send error")
}

func TestReceiverConsumeError(t *testing.T) {
	tc := unhealthyTestChannel{}
	ctc := newCommonTestCase(t, tc)

	td := testdata.GenerateTraces(2)
	batch, err := ctc.testProducer.BatchArrowRecordsFromTraces(td)
	require.NoError(t, err)

	ctc.arrowConsumer.EXPECT().TracesFrom(batch).Times(1).Return([]ptrace.Traces{td}, nil)
	ctc.stream.EXPECT().Send(statusUnavailableFor(batch.BatchId, "consumer unhealthy")).Times(1).Return(nil)

	ctc.start()

	ctc.putBatch(batch, nil)
	assert.EqualValues(t, td, <-ctc.consume)

	err = ctc.cancelAndWait()
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "for %v", err)
}

func TestReceiverInvalidData(t *testing.T) {
	tc := unhealthyTestChannel{}
	ctc := newCommonTestCase(t, tc)

	td := testdata.GenerateTraces(2)
	batch, err := ctc.testProducer.BatchArrowRecordsFromTraces(td)
	require.NoError(t, err)

	ctc.arrowConsumer.EXPECT().TracesFrom(batch).Times(1).Return(nil, fmt.Errorf("test invalid error"))
	ctc.stream.EXPECT().Send(statusInvalidFor(batch.BatchId, "Permanent error: test invalid error")).Times(1).Return(nil)

	ctc.start()
	ctc.putBatch(batch, nil)

	err = ctc.cancelAndWait()
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "for %v", err)
}

func TestReceiverEOF(t *testing.T) {
	tc := healthyTestChannel{}
	ctc := newCommonTestCase(t, tc)

	td := testdata.GenerateTraces(2)
	batch, err := ctc.testProducer.BatchArrowRecordsFromTraces(td)
	require.NoError(t, err)

	// send a sequence of data then simulate closing the connection.
	const times = 10

	ctc.arrowConsumer.EXPECT().TracesFrom(batch).Times(times).Return([]ptrace.Traces{td}, nil)
	ctc.stream.EXPECT().Send(statusOKFor(batch.BatchId)).Times(times).Return(nil)

	ctc.start()

	go func() {
		for i := 0; i < times; i++ {
			ctc.putBatch(batch, nil)
		}

		close(ctc.receive)
	}()

	for i := 0; i < times; i++ {
		assert.EqualValues(t, td, <-ctc.consume)
	}

	err = ctc.wait()
	require.Error(t, err)
	require.True(t, errors.Is(err, io.EOF))
}
