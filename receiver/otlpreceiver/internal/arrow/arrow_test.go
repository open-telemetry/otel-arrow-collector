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
	// 	"context"
	// 	"testing"

	"context"
	"errors"
	"io"
	"testing"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	arrowCollectorMock "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1/mock"
	arrowRecord "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/internal/testdata"
	"go.opentelemetry.io/collector/receiver/otlpreceiver/internal/arrow/mock"
	"go.uber.org/zap/zaptest"
)

type commonTestCase struct {
	ctrl     *gomock.Controller
	telset   component.TelemetrySettings
	service  arrowpb.ArrowStreamServiceClient
	ctxCall  *gomock.Call
	sendCall *gomock.Call
	recvCall *gomock.Call
}

type noisyTest bool

const Noisy noisyTest = true
const NotNoisy noisyTest = false

func newTestTelemetry(t *testing.T, noisy noisyTest) component.TelemetrySettings {
	telset := componenttest.NewNopTelemetrySettings()
	if !noisy {
		telset.Logger = zaptest.NewLogger(t)
	}
	return telset
}

type recvResult struct {
	payload *arrowpb.BatchArrowRecords
	err     error
}

type testChannel struct {
	ch chan recvResult
}

func newTestChannel() *testChannel {
	return &testChannel{
		ch: make(chan recvResult),
	}
}

func (tc *testChannel) put(payload *arrowpb.BatchArrowRecords, err error) {
	tc.ch <- recvResult{
		payload: payload,
		err:     err,
	}
}

func (tc *testChannel) get() (recvResult, bool) {
	r, ok := <-tc.ch
	return r, ok
}

func TestReceiver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ctrl := gomock.NewController(t)
	service := arrowCollectorMock.NewMockArrowStreamService_ArrowStreamServer(ctrl)
	service.EXPECT().Context().AnyTimes().Return(ctx)

	recvCall := service.EXPECT().Recv().Times(0)

	consumers := mock.NewMockConsumers(ctrl)

	settings := component.ReceiverCreateSettings{
		TelemetrySettings: newTestTelemetry(t, NotNoisy),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}
	rcvr := New(config.NewComponentID("arrowtest"), consumers, settings)

	tc := newTestChannel()

	recvCall.AnyTimes().DoAndReturn(func() (*arrowpb.BatchArrowRecords, error) {
		res, ok := tc.get()
		if !ok {
			return nil, io.EOF
		}
		return res.payload, res.err
	})

	streamErr := make(chan error)

	go func() {
		streamErr <- rcvr.ArrowStream(service)
	}()

	td := testdata.GenerateTraces(1)

	prod := arrowRecord.NewProducer()
	batch, err := prod.BatchArrowRecordsFromTraces(td)
	require.NoError(t, err)

	service.EXPECT().Send(gomock.Eq(
		&arrowpb.BatchStatus{
			Statuses: []*arrowpb.StatusMessage{
				{
					BatchId:    batch.BatchId,
					StatusCode: arrowpb.StatusCode_OK,
				},
			},
		},
	)).Times(1).Return(nil)

	tc.put(batch, nil)

	cancel()

	err = <-streamErr
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
}
