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
	"fmt"
	"io"
	"testing"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	arrowCollectorMock "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1/mock"

	"github.com/golang/mock/gomock"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/internal/testdata"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
)

// TODO: More tests requested in PR12
// The following two tests would be interesting to add:
// Case of an invalid Arrow Record
// Case of a very large protobuf message whose objective would be to
// crash the collector (maybe this test is already done at the
// collector level in a more general way).

var (
	waitForReadyOption = []grpc.CallOption{
		grpc.WaitForReady(true),
	}

	// TODO: Test this
	// nowaitForReadyOption = []grpc.CallOption{
	// 	grpc.WaitForReady(false),
	// }

	singleStreamSettings = Settings{
		Enabled:    true,
		NumStreams: 1,
	}

	twoTraces = testdata.GenerateTraces(2)
)

type testChannel interface {
	onRecv(context.Context) func() (*arrowpb.BatchStatus, error)
	onSend(context.Context) func(*arrowpb.BatchArrowRecords) error
	onConnect(context.Context) error
}

type commonTestCase struct {
	ctrl          *gomock.Controller
	telset        component.TelemetrySettings
	serviceClient arrowpb.ArrowStreamServiceClient
	streamCall    *gomock.Call
}

func newTestTelemetry(t *testing.T) component.TelemetrySettings {
	telset := componenttest.NewNopTelemetrySettings()
	telset.Logger = zaptest.NewLogger(t)
	return telset
}

func newCommonTestCase(t *testing.T) *commonTestCase {
	ctrl := gomock.NewController(t)
	telset := newTestTelemetry(t)

	client := arrowCollectorMock.NewMockArrowStreamServiceClient(ctrl)

	streamCall := client.EXPECT().ArrowStream(
		gomock.Any(), // context.Context
		gomock.Any(), // grpc.CallOption
	).Times(0)
	return &commonTestCase{
		ctrl:          ctrl,
		telset:        telset,
		serviceClient: client,
		streamCall:    streamCall,
	}
}

type commonTestStream struct {
	streamClient arrowpb.ArrowStreamService_ArrowStreamClient
	ctxCall      *gomock.Call
	sendCall     *gomock.Call
	recvCall     *gomock.Call
}

func (ctc *commonTestCase) newMockStream(ctx context.Context) *commonTestStream {
	client := arrowCollectorMock.NewMockArrowStreamService_ArrowStreamClient(ctc.ctrl)

	testStream := &commonTestStream{
		streamClient: client,
		ctxCall:      client.EXPECT().Context().AnyTimes().Return(ctx),
		sendCall: client.EXPECT().Send(
			gomock.Any(), // *arrowpb.BatchArrowRecords
		).Times(0),
		recvCall: client.EXPECT().Recv().Times(0),
	}
	return testStream
}

// returnNewStream applies the list of test channels in order to
// construct new streams.  The final entry is re-used for new streams
// when it is reached.
func (ctc *commonTestCase) returnNewStream(hs ...testChannel) func(context.Context, ...grpc.CallOption) (
	arrowpb.ArrowStreamService_ArrowStreamClient,
	error,
) {
	var pos int
	return func(ctx context.Context, _ ...grpc.CallOption) (
		arrowpb.ArrowStreamService_ArrowStreamClient,
		error,
	) {
		h := hs[pos]
		if pos < len(hs) {
			pos++
		}
		if err := h.onConnect(ctx); err != nil {
			return nil, err
		}
		str := ctc.newMockStream(ctx)
		str.sendCall.AnyTimes().DoAndReturn(h.onSend(ctx))
		str.recvCall.AnyTimes().DoAndReturn(h.onRecv(ctx))
		return str.streamClient, nil
	}
}

// healthyTestChannel accepts the connection and returns an OK status immediately.
type healthyTestChannel struct {
	ch chan *arrowpb.BatchArrowRecords
}

func newHealthyTestChannel(sz int) *healthyTestChannel {
	return &healthyTestChannel{
		ch: make(chan *arrowpb.BatchArrowRecords, sz),
	}
}

func (tc *healthyTestChannel) onConnect(_ context.Context) error {
	return nil
}

func (tc *healthyTestChannel) onSend(ctx context.Context) func(*arrowpb.BatchArrowRecords) error {
	return func(req *arrowpb.BatchArrowRecords) error {
		select {
		case tc.ch <- req:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (tc *healthyTestChannel) onRecv(ctx context.Context) func() (*arrowpb.BatchStatus, error) {
	return func() (*arrowpb.BatchStatus, error) {
		select {
		case recv, ok := <-tc.ch:
			if !ok {
				return nil, io.EOF
			}

			bId := recv.BatchId
			return &arrowpb.BatchStatus{
				Statuses: []*arrowpb.StatusMessage{
					{
						BatchId:    bId,
						StatusCode: arrowpb.StatusCode_OK,
					},
				},
			}, nil
		case <-ctx.Done():
			return &arrowpb.BatchStatus{}, ctx.Err()
		}
	}
}

// unresponsiveTestChannel accepts the connection and receives data,
// but never responds with status OK.
type unresponsiveTestChannel struct {
	ch chan struct{}
}

func newUnresponsiveTestChannel() *unresponsiveTestChannel {
	return &unresponsiveTestChannel{
		ch: make(chan struct{}),
	}
}

func (tc *unresponsiveTestChannel) onConnect(_ context.Context) error {
	return nil
}

func (tc *unresponsiveTestChannel) onSend(ctx context.Context) func(*arrowpb.BatchArrowRecords) error {
	return func(req *arrowpb.BatchArrowRecords) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
}

func (tc *unresponsiveTestChannel) onRecv(ctx context.Context) func() (*arrowpb.BatchStatus, error) {
	return func() (*arrowpb.BatchStatus, error) {
		select {
		case <-tc.ch:
			return nil, io.EOF
		case <-ctx.Done():
			return &arrowpb.BatchStatus{}, ctx.Err()
		}
	}
}

func (tc *unresponsiveTestChannel) unblock() {
	close(tc.ch)
}

// unsupportedTestChannel does not accept the connection.
// TODO: Make this match the behavior of the gRPC server for an unrecognized request.
type arrowUnsupportedTestChannel struct {
}

func newArrowUnsupportedTestChannel() *arrowUnsupportedTestChannel {
	return &arrowUnsupportedTestChannel{}
}

func (tc *arrowUnsupportedTestChannel) onConnect(_ context.Context) error {
	return fmt.Errorf("connection failed")
}

func (tc *arrowUnsupportedTestChannel) onSend(ctx context.Context) func(*arrowpb.BatchArrowRecords) error {
	return func(req *arrowpb.BatchArrowRecords) error {
		panic("unreachable")
	}
}

func (tc *arrowUnsupportedTestChannel) onRecv(ctx context.Context) func() (*arrowpb.BatchStatus, error) {
	return func() (*arrowpb.BatchStatus, error) {
		panic("unreachable")
	}
}

// disconnectedTestChannel allows the connection to time out.
type disconnectedTestChannel struct {
	ch chan struct{}
}

func newDisconnectedTestChannel() *disconnectedTestChannel {
	return &disconnectedTestChannel{}
}

func (tc *disconnectedTestChannel) onConnect(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (tc *disconnectedTestChannel) onSend(ctx context.Context) func(*arrowpb.BatchArrowRecords) error {
	return func(req *arrowpb.BatchArrowRecords) error {
		panic("unreachable")
	}
}

func (tc *disconnectedTestChannel) onRecv(ctx context.Context) func() (*arrowpb.BatchStatus, error) {
	return func() (*arrowpb.BatchStatus, error) {
		panic("unreachable")
	}
}
