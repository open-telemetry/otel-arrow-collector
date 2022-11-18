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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type exporterTestCase struct {
	*commonTestCase
	exporter *Exporter
}

func newExporterTestCase(t *testing.T, noisy noisyTest, arrowset Settings) *exporterTestCase {
	ctc := newCommonTestCase(t, noisy)
	exp := NewExporter(arrowset, ctc.telset, ctc.serviceClient, nil)

	return &exporterTestCase{
		commonTestCase: ctc,
		exporter:       exp,
	}
}

// TestArrowExporterSuccess tests a single Send through a healthy channel.
func TestArrowExporterSuccess(t *testing.T) {
	for _, data := range []interface{}{twoTraces, twoMetrics, twoLogs} {
		tc := newExporterTestCase(t, NotNoisy, singleStreamSettings)
		channel := newHealthyTestChannel(1)

		tc.streamCall.Times(1).DoAndReturn(tc.returnNewStream(channel))

		ctx := context.Background()
		require.NoError(t, tc.exporter.Start(ctx))

		consumer, err := tc.exporter.GetStream(ctx)
		require.NoError(t, err)

		require.NoError(t, consumer.SendAndWait(ctx, data))

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
