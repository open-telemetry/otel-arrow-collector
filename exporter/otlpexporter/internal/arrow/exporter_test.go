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
	"sync"
	"testing"
	"time"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	arrowRecord "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record"
	arrowRecordMock "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/internal/testdata"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type exporterTestCase struct {
	*commonTestCase
	exporter *Exporter
}

func newExporterTestCase(t *testing.T, noisy noisyTest, arrowset Settings) *exporterTestCase {
	ctc := newCommonTestCase(t, noisy)
	exp := NewExporter(arrowset, func() arrowRecord.ProducerAPI {
		// Mock the close function, use a real producer for testing dataflow.
		prod := arrowRecordMock.NewMockProducerAPI(ctc.ctrl)
		real := arrowRecord.NewProducer()

		prod.EXPECT().BatchArrowRecordsFromTraces(gomock.Any()).AnyTimes().DoAndReturn(
			real.BatchArrowRecordsFromTraces)
		prod.EXPECT().BatchArrowRecordsFromLogs(gomock.Any()).AnyTimes().DoAndReturn(
			real.BatchArrowRecordsFromLogs)
		prod.EXPECT().BatchArrowRecordsFromMetrics(gomock.Any()).AnyTimes().DoAndReturn(
			real.BatchArrowRecordsFromMetrics)
		prod.EXPECT().Close().Times(1).Return(nil)
		return prod
	}, ctc.telset, ctc.serviceClient, nil)

	return &exporterTestCase{
		commonTestCase: ctc,
		exporter:       exp,
	}
}

func statusOKFor(id string) *arrowpb.BatchStatus {
	return &arrowpb.BatchStatus{
		Statuses: []*arrowpb.StatusMessage{
			{
				BatchId:    id,
				StatusCode: arrowpb.StatusCode_OK,
			},
		},
	}
}

// TestArrowExporterSuccess tests a single Send through a healthy channel.
func TestArrowExporterSuccess(t *testing.T) {
	for _, inputData := range []interface{}{twoTraces, twoMetrics, twoLogs} {
		tc := newExporterTestCase(t, NotNoisy, singleStreamSettings)
		channel := newHealthyTestChannel(1)

		tc.streamCall.Times(1).DoAndReturn(tc.returnNewStream(channel))

		ctx := context.Background()
		require.NoError(t, tc.exporter.Start(ctx))

		consumer, err := tc.exporter.GetStream(ctx)
		require.NoError(t, err)

		var wg sync.WaitGroup
		var outputData *arrowpb.BatchArrowRecords
		wg.Add(1)
		go func() {
			defer wg.Done()
			outputData = <-channel.sent
			channel.recv <- statusOKFor(outputData.BatchId)
		}()

		require.NoError(t, consumer.SendAndWait(ctx, inputData))

		wg.Wait()

		testCon := arrowRecord.NewConsumer()
		switch testData := inputData.(type) {
		case ptrace.Traces:
			traces, err := testCon.TracesFrom(outputData)
			require.NoError(t, err)
			require.Equal(t, []ptrace.Traces{testData}, traces)
		case plog.Logs:
			logs, err := testCon.LogsFrom(outputData)
			require.NoError(t, err)
			require.Equal(t, []plog.Logs{testData}, logs)
		case pmetric.Metrics:
			metrics, err := testCon.MetricsFrom(outputData)
			require.NoError(t, err)
			require.Equal(t, []pmetric.Metrics{testData}, metrics)
		}

		require.NoError(t, tc.exporter.Shutdown(ctx))
	}
}

// TestArrowExporterTimeout tests that single slow Send leads to context canceled.
func TestArrowExporterTimeout(t *testing.T) {
	tc := newExporterTestCase(t, NotNoisy, singleStreamSettings)
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
	tc := newExporterTestCase(t, NotNoisy, singleStreamSettings)
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
	tc := newExporterTestCase(t, NotNoisy, singleStreamSettings)
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
	tc := newExporterTestCase(t, NotNoisy, singleStreamSettings)
	channel0 := newUnresponsiveTestChannel()
	channel1 := newHealthyTestChannel(1)

	tc.streamCall.AnyTimes().DoAndReturn(tc.returnNewStream(channel0, channel1))

	bg := context.Background()
	require.NoError(t, tc.exporter.Start(bg))

	go func() {
		time.Sleep(200 * time.Millisecond)
		channel0.unblock()
	}()

	var wg sync.WaitGroup
	var outputData *arrowpb.BatchArrowRecords
	wg.Add(1)
	go func() {
		defer wg.Done()
		outputData = <-channel1.sent
		channel1.recv <- statusOKFor(outputData.BatchId)
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
	wg.Wait()

	require.NoError(t, tc.exporter.Shutdown(bg))
}

// TestArrowExporterStreamRace reproduces the situation needed for a
// race between stream send and stream cancel, causing it to fully
// exercise the removeReady() code path.
func TestArrowExporterStreamRace(t *testing.T) {
	// Two streams ensures every possibility.
	tc := newExporterTestCase(t, Noisy, twoStreamsSettings)

	tc.streamCall.AnyTimes().DoAndReturn(tc.repeatedNewStream(func() testChannel {
		tc := newUnresponsiveTestChannel()
		// Immediately unblock to return the EOF to the stream
		// receiver and shut down the stream.
		go tc.unblock()
		return tc
	}))

	bg := context.Background()
	require.NoError(t, tc.exporter.Start(bg))

	for tries := 0; tries < 1000; tries++ {
		stream, err := tc.exporter.GetStream(bg)
		require.NotNil(t, stream)
		require.NoError(t, err)

		err = stream.SendAndWait(bg, twoTraces)

		require.Error(t, err)
		require.True(t, errors.Is(err, ErrStreamRestarting))
	}

	require.NoError(t, tc.exporter.Shutdown(bg))
}

// TestArrowExporterStreaming tests 10 sends in a row.
func TestArrowExporterStreaming(t *testing.T) {
	tc := newExporterTestCase(t, NotNoisy, singleStreamSettings)
	channel := newHealthyTestChannel(1)

	tc.streamCall.AnyTimes().DoAndReturn(tc.returnNewStream(channel))

	bg := context.Background()
	require.NoError(t, tc.exporter.Start(bg))

	var expectOutput []ptrace.Traces
	var actualOutput []ptrace.Traces
	testCon := arrowRecord.NewConsumer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for data := range channel.sent {
			traces, err := testCon.TracesFrom(data)
			require.NoError(t, err)
			require.Equal(t, 1, len(traces))
			actualOutput = append(actualOutput, traces[0])
			channel.recv <- statusOKFor(data.BatchId)
		}
	}()

	for times := 0; times < 10; times++ {
		stream, err := tc.exporter.GetStream(bg)
		require.NotNil(t, stream)
		require.NoError(t, err)

		input := testdata.GenerateTraces(2)
		expectOutput = append(expectOutput, input)

		err = stream.SendAndWait(bg, input)
		require.NoError(t, err)
	}
	// Stop the test conduit started above.  If the sender were
	// still sending, it would panic on a closed channel.
	close(channel.sent)
	wg.Wait()

	require.Equal(t, expectOutput, actualOutput)
	require.NoError(t, tc.exporter.Shutdown(bg))
}
