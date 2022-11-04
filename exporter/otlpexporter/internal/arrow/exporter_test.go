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

func (tc *exporterTestCase) returnNewStream(h testChannel) func(context.Context, ...grpc.CallOption) (
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
		str := tc.newStream(ctx)
		str.sendCall.Times(1).DoAndReturn(h.onSend(ctx))
		str.recvCall.AnyTimes().DoAndReturn(h.onRecv(ctx))
		return str.client, nil
	}
}

// healthy

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

// unresponsive

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

// unsupported

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

// disconnected

type disconnectedTestChannel struct {
	ch chan struct{}
}

func newDisconnectedTestChannel() *disconnectedTestChannel {
	return &disconnectedTestChannel{
		ch: make(chan struct{}),
	}
}

func (tc *disconnectedTestChannel) onConnect(ctx context.Context) error {
	select {
	case <-tc.ch:
		return fmt.Errorf("do not connect")
	case <-ctx.Done():
		return ctx.Err()
	}
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
