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
	"io"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/internal/testdata"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	arrowMock "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
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

type exporterTestCase struct {
	ctrl       *gomock.Controller
	streamCall *gomock.Call
	exporter   *Exporter
}

func newExporterTestCase(t *testing.T, arrowset Settings) *exporterTestCase {
	ctrl := gomock.NewController(t)

	client := arrowMock.NewMockArrowStreamServiceClient(ctrl)

	telset := componenttest.NewNopTelemetrySettings()
	telset.Logger = zaptest.NewLogger(t)

	exp := NewExporter(arrowset, telset, client, waitForReadyOption)

	streamCall := client.EXPECT().ArrowStream(
		gomock.Any(), // context.Context
		gomock.Any(), // grpc.CallOption
	)

	return &exporterTestCase{
		ctrl:       ctrl,
		streamCall: streamCall,
		exporter:   exp,
	}
}

type exporterTestStream struct {
	client   arrowpb.ArrowStreamService_ArrowStreamClient
	ctxCall  *gomock.Call
	sendCall *gomock.Call
	recvCall *gomock.Call
}

func (tc *exporterTestCase) newStream(ctx context.Context) *exporterTestStream {
	client := arrowMock.NewMockArrowStreamService_ArrowStreamClient(tc.ctrl)

	testStream := &exporterTestStream{
		client:  client,
		ctxCall: client.EXPECT().Context().AnyTimes().Return(ctx),
		sendCall: client.EXPECT().Send(
			gomock.Any(), // *arrowpb.BatchArrowRecords
		),
		recvCall: client.EXPECT().Recv(),
	}
	return testStream
}

// returnNewStream applies the list of test channels in order to
// construct new streams.  The final entry is re-used for new streams
// when it is reached.
func (tc *exporterTestCase) returnNewStream(hs ...testChannel) func(context.Context, ...grpc.CallOption) (
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
		str := tc.newStream(ctx)
		str.sendCall.AnyTimes().DoAndReturn(h.onSend(ctx))
		str.recvCall.AnyTimes().DoAndReturn(h.onRecv(ctx))
		return str.client, nil
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

// TestArrowExporterSuccess tests a single Send through a healthy channel.
func TestArrowExporterSuccess(t *testing.T) {
	tc := newExporterTestCase(t, singleStreamSettings)
	channel := newHealthyTestChannel(1)

	tc.streamCall.Times(1).DoAndReturn(tc.returnNewStream(channel))

	ctx := context.Background()
	require.NoError(t, tc.exporter.Start(ctx))

	consumer, err := tc.exporter.GetStream(ctx)
	require.NoError(t, err)

	require.NoError(t, consumer.SendAndWait(ctx, twoTraces))

	require.NoError(t, tc.exporter.Shutdown(ctx))
}

// TestArrowExporterTimeout tests that single slow Send leads to context canceled.
func TestArrowExporterTimeout(t *testing.T) {
	tc := newExporterTestCase(t, singleStreamSettings)
	channel := newUnresponsiveTestChannel()

	tc.streamCall.Times(1).DoAndReturn(tc.returnNewStream(channel))

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, tc.exporter.Start(ctx))

	consumer, err := tc.exporter.GetStream(ctx)
	require.NoError(t, err)

	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	err = consumer.SendAndWait(ctx, twoTraces)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))

	require.NoError(t, tc.exporter.Shutdown(ctx))
}

// TestArrowExporterDowngrade tests that if the connetions fail fast
// (TODO in a precisely appropriate way) the connection is downgraded
// without error.
func TestArrowExporterDowngrade(t *testing.T) {
	tc := newExporterTestCase(t, singleStreamSettings)
	channel := newArrowUnsupportedTestChannel()

	tc.streamCall.AnyTimes().DoAndReturn(tc.returnNewStream(channel))

	bg := context.Background()
	require.NoError(t, tc.exporter.Start(bg))

	stream, err := tc.exporter.GetStream(bg)
	require.Nil(t, stream)
	require.NoError(t, err)

	// TODO: test the logger was used to report "downgrading"

	require.NoError(t, tc.exporter.Shutdown(bg))
}

// TestArrowExporterConnectTimeout tests that an error is returned to
// the caller if the response does not arrive in time.
func TestArrowExporterConnectTimeout(t *testing.T) {
	tc := newExporterTestCase(t, singleStreamSettings)
	channel := newDisconnectedTestChannel()

	tc.streamCall.AnyTimes().DoAndReturn(tc.returnNewStream(channel))

	bg := context.Background()
	ctx, cancel := context.WithCancel(bg)
	require.NoError(t, tc.exporter.Start(bg))

	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	stream, err := tc.exporter.GetStream(ctx)
	require.Nil(t, stream)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))

	require.NoError(t, tc.exporter.Shutdown(bg))
}

// TestArrowExporterStreamFailure tests that a single stream failure
// followed by a healthy stream.
func TestArrowExporterStreamFailure(t *testing.T) {
	tc := newExporterTestCase(t, singleStreamSettings)
	channel0 := newUnresponsiveTestChannel()
	channel1 := newHealthyTestChannel(1)

	tc.streamCall.AnyTimes().DoAndReturn(tc.returnNewStream(channel0, channel1))

	bg := context.Background()
	require.NoError(t, tc.exporter.Start(bg))

	go func() {
		time.Sleep(200 * time.Millisecond)
		channel0.unblock()
	}()

	for times := 0; times < 2; times++ {
		stream, err := tc.exporter.GetStream(bg)
		require.NotNil(t, stream)
		require.NoError(t, err)

		err = stream.SendAndWait(bg, twoTraces)

		if times == 0 {
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrStreamRestarting))
		} else {
			require.NoError(t, err)
		}
	}

	require.NoError(t, tc.exporter.Shutdown(bg))
}
