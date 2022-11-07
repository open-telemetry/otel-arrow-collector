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
	"time"

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

// start runs a test stream according to the behavior of testChannel.
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

// connectTestStream returns the stream under test from the common test's mock ArrowStream().
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

// get returns the stream via the prioritizer it is registered with.
func (tc *streamTestCase) get() *Stream {
	return <-tc.prioritizer.readyChannel()
}

// TestStreamEncodeError verifies that an encoder error in the sender
// yields a permanent error.
func TestStreamEncodeError(t *testing.T) {
	tc := newStreamTestCase(t)

	testErr := fmt.Errorf("test encode error")
	tc.fromTracesCall.Times(1).Return(nil, testErr)

	cleanup := tc.start(newHealthyTestChannel(1))
	defer cleanup()

	// sender should get a permanent testErr
	err := (<-tc.prioritizer.readyChannel()).SendAndWait(tc.bgctx, twoTraces)
	require.Error(t, err)
	require.True(t, errors.Is(err, testErr))
	require.True(t, consumererror.IsPermanent(err))
}

// TestStreamUnknownBatchError verifies that the stream reader handles
// a unknown BatchID.
func TestStreamUnknownBatchError(t *testing.T) {
	tc := newStreamTestCase(t)

	tc.fromTracesCall.Times(1).Return(oneBatch, nil)

	channel := newUnknownBatchIDTestChannel()
	cleanup := tc.start(channel)
	defer cleanup()

	go func() {
		time.Sleep(200 * time.Millisecond)
		channel.unblock()
	}()
	// sender should get ErrStreamRestarting
	err := tc.get().SendAndWait(tc.bgctx, twoTraces)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrStreamRestarting))
}

// TestStreamSendError verifies that the stream reader handles a
// Send() error.
func TestStreamSendError(t *testing.T) {
	tc := newStreamTestCase(t)

	tc.fromTracesCall.Times(1).Return(oneBatch, nil)

	channel := newSendErrorTestChannel()
	cleanup := tc.start(channel)
	defer cleanup()

	go func() {
		time.Sleep(200 * time.Millisecond)
		channel.unblock()
	}()
	// sender should get ErrStreamRestarting
	err := tc.get().SendAndWait(tc.bgctx, twoTraces)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrStreamRestarting))
}
