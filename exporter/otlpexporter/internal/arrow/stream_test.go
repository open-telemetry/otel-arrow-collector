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
	"errors"
	"fmt"
	"sync"
	"testing"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	arrowRecordMock "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"google.golang.org/grpc"
)

var oneBatch = &arrowpb.BatchArrowRecords{
	BatchId: "b1",
}

type streamTestCase struct {
	*commonTestCase
	*commonTestStream

	producer       *arrowRecordMock.MockProducerAPI
	prioritizer    *streamPrioritizer
	bgctx          context.Context
	bgcancel       context.CancelFunc
	fromTracesCall *gomock.Call
	stream         *Stream
}

func newStreamTestCase(t *testing.T) *streamTestCase {
	ctrl := gomock.NewController(t)
	producer := arrowRecordMock.NewMockProducerAPI(ctrl)
	aset := singleStreamSettings

	bg, cancel := context.WithCancel(context.Background())
	prio := newStreamPrioritizer(bg, aset)

	ctc := newCommonTestCase(t)
	cts := ctc.newMockStream(bg)

	stream := newStream(producer, prio, ctc.telset)

	fromTracesCall := producer.EXPECT().BatchArrowRecordsFromTraces(gomock.Any()).Times(0)

	return &streamTestCase{
		commonTestCase:   ctc,
		commonTestStream: cts,
		producer:         producer,
		prioritizer:      prio,
		bgctx:            bg,
		bgcancel:         cancel,
		stream:           stream,
		fromTracesCall:   fromTracesCall,
	}
}

func (tc *streamTestCase) start(channel testChannel) func() {
	tc.streamCall.Times(1).DoAndReturn(tc.connectTestStream(channel))

	var wait sync.WaitGroup
	wait.Add(1)

	go func() {
		defer wait.Done()
		tc.stream.run(tc.bgctx, tc.serviceClient, waitForReadyOption)
	}()

	return func() {
		tc.bgcancel()
		wait.Wait()
	}
}

func (tc *streamTestCase) connectTestStream(h testChannel) func(context.Context, ...grpc.CallOption) (
	arrowpb.ArrowStreamService_ArrowStreamClient,
	error,
) {
	return func(ctx context.Context, _ ...grpc.CallOption) (
		arrowpb.ArrowStreamService_ArrowStreamClient,
		error,
	) {
		if err := h.onConnect(ctx); err != nil {
			return nil, err
		}
		tc.sendCall.AnyTimes().DoAndReturn(h.onSend(ctx))
		tc.recvCall.AnyTimes().DoAndReturn(h.onRecv(ctx))
		return tc.streamClient, nil
	}
}

// TestStreamEncodeError verifies that an encoder error in the sender
// yields a permanent error.
func TestStreamEncodeError(t *testing.T) {
	tc := newStreamTestCase(t)

	testErr := fmt.Errorf("test encode error")
	tc.fromTracesCall.Times(1).Return(nil, testErr)

	cleanup := tc.start(newHealthyTestChannel(1))
	defer cleanup()

	err := tc.stream.SendAndWait(tc.bgctx, twoTraces)
	require.Error(t, err)
	require.True(t, errors.Is(err, testErr))
	require.True(t, consumererror.IsPermanent(err))
}

// TestStreamUnknownBatchError verifies that the stream reader
// handles a missing BatchID e.g., caused by duplicate response.
func TestStreamUnknownBatchError(t *testing.T) {
	channel := newHealthyTestChannel(1)
	tc := newStreamTestCase(t)

	tc.fromTracesCall.Times(1).Return(oneBatch, nil)
	tc.sendCall.AnyTimes().Return(nil)
	tc.recvCall.AnyTimes().Return(&arrowpb.BatchStatus{
		Statuses: []*arrowpb.StatusMessage{
			{
				BatchId:    "unknown",
				StatusCode: arrowpb.StatusCode_OK,
			},
		},
	}, nil)

	cleanup := tc.start(channel)
	defer cleanup()

	err := tc.stream.SendAndWait(tc.bgctx, twoTraces)
	require.Error(t, err)
}
