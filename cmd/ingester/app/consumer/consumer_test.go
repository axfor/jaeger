// Copyright (c) 2018 The Jaeger Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consumer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kmocks "github.com/jaegertracing/jaeger/cmd/ingester/app/consumer/mocks"
	"github.com/jaegertracing/jaeger/cmd/ingester/app/processor"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/metrics"
	"github.com/jaegertracing/jaeger/pkg/testutils"
	"github.com/jaegertracing/jaeger/plugin/storage/kafka"
	"github.com/jaegertracing/jaeger/plugin/storage/memory"
)

//go:generate mockery -dir ../../../../pkg/kafka/config/ -name Consumer

const (
	topic     = "jaegertracing_consumer"
	group     = "jaegertracing_consumer_group"
	partition = int32(316)
	msgOffset = int64(1111110111111)
)

func TestConstructor(t *testing.T) {
	newConsumer, err := New(Params{MetricsFactory: metrics.NullFactory})
	assert.NoError(t, err)
	assert.NotNil(t, newConsumer)
}

func TestSaramaConsumerWrapper_MarkPartitionOffset(t *testing.T) {
	sc := &kmocks.Consumer{}
	metadata := "meatbag"
	sc.On("MarkPartitionOffset", topic, partition, msgOffset, metadata).Return()
	sc.MarkPartitionOffset(topic, partition, msgOffset, metadata)
	sc.AssertCalled(t, "MarkPartitionOffset", topic, partition, msgOffset, metadata)
}

// NewTestConfig returns a config meant to be used by tests.
// Due to inconsistencies with the request versions the clients send using the default Kafka version
// and the response versions our mocks use, we default to the minimum Kafka version in most tests
func NewTestConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.MinVersion
	return config
}

type Store struct {
	store    *memory.Store
	consumer *Consumer
	done     sync.WaitGroup
	t        *testing.T
}

func (s *Store) WriteSpan(ctx context.Context, span *model.Span) error {
	err := s.store.WriteSpan(ctx, span)
	go func() {
		s.consumer.Close()
		s.done.Done()
	}()
	require.NoError(s.t, err)
	return err
}

func TestGroupConsumer(t *testing.T) {
	config := NewTestConfig()
	config.ClientID = t.Name()
	config.Version = sarama.V2_0_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Retry.Max = 2
	config.Consumer.Offsets.AutoCommit.Enable = false

	msg1, err := newSampleSpan(model.NewTraceID(1, 2), model.NewSpanID(1))
	require.NoError(t, err)
	msg2, err := newSampleSpan(model.NewTraceID(2, 3), model.NewSpanID(2))
	require.NoError(t, err)

	broker0 := sarama.NewMockBroker(t, 0)
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader(topic, 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(topic, 0, sarama.OffsetOldest, 0).
			SetOffset(topic, 0, sarama.OffsetNewest, 1),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, group, broker0),
		"HeartbeatRequest": sarama.NewMockHeartbeatResponse(t),
		"JoinGroupRequest": sarama.NewMockSequence(
			sarama.NewMockJoinGroupResponse(t).SetError(sarama.ErrOffsetsLoadInProgress),
			sarama.NewMockJoinGroupResponse(t).SetGroupProtocol(sarama.RangeBalanceStrategyName),
		),
		"SyncGroupRequest": sarama.NewMockSequence(
			sarama.NewMockSyncGroupResponse(t).SetError(sarama.ErrOffsetsLoadInProgress),
			sarama.NewMockSyncGroupResponse(t).SetMemberAssignment(
				&sarama.ConsumerGroupMemberAssignment{
					Version: 0,
					Topics: map[string][]int32{
						topic: {0},
					},
				}),
		),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
			SetOffset(group, topic, 0, 0, "", sarama.ErrNoError).
			SetOffset(group, topic, 0, 1, "", sarama.ErrNoError).
			SetError(sarama.ErrNoError),
		"FetchRequest": sarama.NewMockSequence(
			sarama.NewMockFetchResponse(t, 1).
				SetMessage(topic, 0, 0, sarama.StringEncoder(msg1)).
				SetMessage(topic, 0, 1, sarama.StringEncoder(msg2)),
			sarama.NewMockFetchResponse(t, 1),
		),
	})

	saramaConsumer, err := sarama.NewConsumerGroup([]string{broker0.Addr()}, group, config)
	require.NoError(t, err)

	defer func() { _ = saramaConsumer.Close() }()

	unmarshaller := kafka.NewJSONUnmarshaller()
	innerSpanWriter := memory.NewStore()

	spanWriter := &Store{
		store: innerSpanWriter,
	}
	spanWriter.done.Add(1)

	spParams := processor.SpanProcessorParams{
		Writer:       spanWriter,
		Unmarshaller: unmarshaller,
	}

	spanProcessor := processor.NewSpanProcessor(spParams)

	logger, logBuf := testutils.NewLogger()
	factoryParams := ProcessorFactoryParams{
		Topic:          topic,
		Parallelism:    1,
		SaramaConsumer: saramaConsumer,
		BaseProcessor:  spanProcessor,
		Logger:         logger,
		Factory:        metrics.NullFactory,
	}

	processorFactory, err := NewProcessorFactory(factoryParams)
	require.NoError(t, err)

	consumerParams := Params{
		InternalConsumer:      saramaConsumer,
		ProcessorFactory:      *processorFactory,
		MetricsFactory:        metrics.NullFactory,
		Logger:                logger,
		DeadlockCheckInterval: 0,
	}

	consumer, err := New(consumerParams)
	require.NoError(t, err)

	spanWriter.consumer = consumer
	spanWriter.t = t

	consumer.Start()
	consumer.WaitReady()

	t.Log("Consumer is ready and wait message")

	spanWriter.done.Wait()

	t.Logf("Consumer all logs: %s", logBuf.String())
}

func newSampleSpan(traceID model.TraceID, spanID model.SpanID) (string, error) {
	sampleTags := model.KeyValues{
		model.String("someStringTagKey", "someStringTagValue"),
	}
	sampleSpan := &model.Span{
		TraceID:       traceID,
		SpanID:        spanID,
		OperationName: "someOperationName",
		References: []model.SpanRef{
			{
				TraceID: traceID,
				SpanID:  spanID,
				RefType: model.ChildOf,
			},
		},
		Flags:     model.Flags(1),
		StartTime: model.EpochMicrosecondsAsTime(55555),
		Duration:  model.MicrosecondsAsDuration(50000),
		Tags:      sampleTags,
		Logs: []model.Log{
			{
				Timestamp: model.EpochMicrosecondsAsTime(12345),
				Fields:    sampleTags,
			},
		},
		Process: &model.Process{
			ServiceName: "someServiceName",
			Tags:        sampleTags,
		},
	}

	m := &jsonpb.Marshaler{}
	msg, err := m.MarshalToString(sampleSpan)
	if err != nil {
		return "", err
	}
	return msg, nil
}

func TestGroupConsumerWithDeadlockDetector(t *testing.T) {
	config := NewTestConfig()
	config.ClientID = t.Name()
	config.Version = sarama.V2_0_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Retry.Max = 2
	config.Consumer.Offsets.AutoCommit.Enable = false

	broker0 := sarama.NewMockBroker(t, 0)
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader(topic, 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(topic, 0, sarama.OffsetOldest, 0).
			SetOffset(topic, 0, sarama.OffsetNewest, 1),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, group, broker0),
		"HeartbeatRequest": sarama.NewMockHeartbeatResponse(t),
		"JoinGroupRequest": sarama.NewMockSequence(
			sarama.NewMockJoinGroupResponse(t).SetError(sarama.ErrOffsetsLoadInProgress),
			sarama.NewMockJoinGroupResponse(t).SetGroupProtocol(sarama.RangeBalanceStrategyName),
		),
		"SyncGroupRequest": sarama.NewMockSequence(
			sarama.NewMockSyncGroupResponse(t).SetError(sarama.ErrOffsetsLoadInProgress),
			sarama.NewMockSyncGroupResponse(t).SetMemberAssignment(
				&sarama.ConsumerGroupMemberAssignment{
					Version: 0,
					Topics: map[string][]int32{
						topic: {0},
					},
				}),
		),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).SetOffset(
			group, topic, 0, 0, "", sarama.ErrNoError,
		).SetError(sarama.ErrNoError),
		"FetchRequest": sarama.NewMockSequence(
			sarama.NewMockFetchResponse(t, 1),
		),
	})

	saramaConsumer, err := sarama.NewConsumerGroup([]string{broker0.Addr()}, group, config)
	require.NoError(t, err)

	defer func() { _ = saramaConsumer.Close() }()

	unmarshaller := kafka.NewJSONUnmarshaller()
	innerSpanWriter := memory.NewStore()

	spanWriter := &Store{
		store: innerSpanWriter,
	}
	spanWriter.done.Add(1)

	spParams := processor.SpanProcessorParams{
		Writer:       spanWriter,
		Unmarshaller: unmarshaller,
	}

	spanProcessor := processor.NewSpanProcessor(spParams)

	logger, logBuf := testutils.NewLogger()
	factoryParams := ProcessorFactoryParams{
		Topic:          topic,
		Parallelism:    1,
		SaramaConsumer: saramaConsumer,
		BaseProcessor:  spanProcessor,
		Logger:         logger,
		Factory:        metrics.NullFactory,
	}

	processorFactory, err := NewProcessorFactory(factoryParams)
	require.NoError(t, err)

	consumerParams := Params{
		InternalConsumer:      saramaConsumer,
		ProcessorFactory:      *processorFactory,
		MetricsFactory:        metrics.NullFactory,
		Logger:                logger,
		DeadlockCheckInterval: time.Second,
	}

	consumer, err := New(consumerParams)
	require.NoError(t, err)

	spanWriter.consumer = consumer
	spanWriter.t = t

	consumer.Start()
	consumer.CloseDeadlockDetector()

	consumer.WaitReady()
	t.Log("Consumer is ready and wait message")

	consumer.WaitDeadlockNotify()
	consumer.Close()

	t.Logf("Consumer all logs: %s", logBuf.String())
}
