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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/cmd/ingester/app/processor"
	"github.com/jaegertracing/jaeger/pkg/kafka/consumer"
	"github.com/jaegertracing/jaeger/pkg/metrics"
)

// Params are the parameters of a Consumer
type Params struct {
	ProcessorFactory      ProcessorFactory
	MetricsFactory        metrics.Factory
	Logger                *zap.Logger
	InternalConsumer      consumer.Consumer
	DeadlockCheckInterval time.Duration
}

// Consumer uses sarama to consume and handle messages from kafka
type Consumer struct {
	metricsFactory metrics.Factory
	logger         *zap.Logger

	internalConsumer consumer.Consumer
	processorFactory ProcessorFactory

	deadlockDetector   deadlockDetector
	deadlockNotifyCh   chan struct{}
	deadlockNotifyOnce sync.Once

	partitionsHeld      atomic.Int64
	partitionsHeldGauge metrics.Gauge

	consumerReady *consumerReady

	doneWg sync.WaitGroup

	topic  string
	cancel context.CancelFunc
}

type consumerReady struct {
	readyCh   chan struct{}
	closeOnce sync.Once
}

func (c *consumerReady) waitReady() {
	<-c.readyCh
}

func (c *consumerReady) markReady() {
	c.close()
}

func (c *consumerReady) close() {
	c.closeOnce.Do(func() {
		close(c.readyCh)
	})
}

// New is a constructor for a Consumer
func New(params Params) (*Consumer, error) {
	deadlockDetector := newDeadlockDetector(params.MetricsFactory, params.Logger, params.DeadlockCheckInterval)
	// new Consumer
	return &Consumer{
		metricsFactory:      params.MetricsFactory,
		logger:              params.Logger,
		internalConsumer:    params.InternalConsumer,
		processorFactory:    params.ProcessorFactory,
		deadlockDetector:    deadlockDetector,
		deadlockNotifyCh:    make(chan struct{}, 1),
		partitionsHeldGauge: partitionsHeldGauge(params.MetricsFactory),
		consumerReady: &consumerReady{
			readyCh: make(chan struct{}, 1),
		},
		topic: params.ProcessorFactory.topic,
	}, nil
}

// Start begins consuming messages in a go routine
func (c *Consumer) Start() {
	c.doStart(true)
}

// Start begins consuming messages in a go routine and consumer is running
func (c *Consumer) StartWithReady() {
	c.Start()
	c.consumerReady.waitReady()
}

// Start begins consuming and wait is running and disable global deadlock detector,
// There are two deadlock detectors, one global and one specially partition
// If the consumer is running, the specially partition deadlock detector takes effect
func (c *Consumer) startWithReadyAndDisableGlobalDeadlockDetector() {
	c.doStart(false)
	c.consumerReady.waitReady()
}

// doStart begins consuming messages in a go routine
func (c *Consumer) doStart(startAllPartitionDeadlockDetector bool) {
	if startAllPartitionDeadlockDetector {
		// all partition deadlock detector
		c.deadlockDetector.start()
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.doneWg.Add(1)
	go func() {
		defer c.doneWg.Done()
		c.startConsume(ctx)
	}()

	go c.handleErrors(ctx)
}

func (c *Consumer) startConsume(ctx context.Context) {
	topic := []string{c.topic}
	// reference consumerReady and make sure to close it, because later consumerReady may reference a new object
	ready := c.consumerReady
	defer ready.close()

	for {
		select {
		case <-ctx.Done():
			c.logger.Error("ctx canceled")
			return
		default:
			c.logger.Info("Topic", zap.Strings("topic", topic))
			if err := c.internalConsumer.Consume(ctx, topic, c); err != nil {
				c.logger.Error("Error from consumer", zap.Error(err))
			}

			c.consumerReady.close()

			c.consumerReady = &consumerReady{
				readyCh: make(chan struct{}, 1),
			}

			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				if c.cancel != nil {
					c.cancel()
				}
			}
		}
	}
}

// markDeadlock mark consume is deadlocked
func (c *Consumer) markDeadlock() {
	c.deadlockNotifyOnce.Do(func() {
		close(c.deadlockNotifyCh)
	})
}

// Deadlock return a consume deadlock notify chan
func (c *Consumer) Deadlock() <-chan struct{} {
	return c.deadlockNotifyCh
}

// Close closes the Consumer and underlying sarama consumer
func (c *Consumer) Close() error {
	// Close the internal consumer, which will close each partition consumers' message and error channels.
	c.logger.Info("Closing parent consumer")
	err := c.internalConsumer.Close()
	c.deadlockDetector.close()

	if c.cancel != nil {
		c.cancel()
	}

	c.logger.Debug("Waiting for messages and errors to be handled")
	c.doneWg.Wait()

	return err
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	c.consumerReady.markReady()
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.partitionMetrics(claim.Partition()).startCounter.Inc(1)

	return c.startMessagesLoop(session, claim)
}

func (c *Consumer) startMessagesLoop(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	return c.handleMessages(session, claim)
}

// handleMessages starting message handler
func (c *Consumer) handleMessages(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.logger.Info("Starting message handler", zap.Int32("partition", claim.Partition()))

	c.partitionsHeld.Add(1)
	c.partitionsHeldGauge.Update(c.partitionsHeld.Load())

	msgMetrics := c.newMsgMetrics(claim.Partition())
	var msgProcessor processor.SpanProcessor = nil

	partitionDeadlockDetector := c.deadlockDetector.startMonitoringForPartition(claim.Partition())

	defer func() {
		partitionDeadlockDetector.close()
		c.closePartition(claim)
		c.partitionsHeld.Add(-1)
		c.partitionsHeldGauge.Update(c.partitionsHeld.Load())
	}()

	for {
		select {
		case msg := <-claim.Messages():

			c.logger.Debug("Got msg", zap.Any("msg", msg))
			msgMetrics.counter.Inc(1)
			msgMetrics.offsetGauge.Update(msg.Offset)
			msgMetrics.lagGauge.Update(claim.HighWaterMarkOffset() - msg.Offset - 1)
			partitionDeadlockDetector.incrementMsgCount()

			if msgProcessor == nil {
				msgProcessor = c.processorFactory.new(session, claim, msg.Offset-1)
				defer msgProcessor.Close()
			}

			msgProcessor.Process(saramaMessageWrapper{msg})

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			c.logger.Info("Session done", zap.Int32("partition", claim.Partition()))
			return session.Context().Err()
		case <-partitionDeadlockDetector.closePartitionChannel():
			c.logger.Info("Closing partition due to inactivity", zap.Int32("partition", claim.Partition()))
			c.markDeadlock()
			return fmt.Errorf("closing partition[%d] due to inactivity", claim.Partition())
		}
	}
}

// closePartition close partition of consumer
func (c *Consumer) closePartition(claim sarama.ConsumerGroupClaim) {
	c.logger.Info("Closing partition consumer", zap.Int32("partition", claim.Partition()))
	c.partitionMetrics(claim.Partition()).closeCounter.Inc(1)
}

// handleErrors handles incoming Kafka consumer errors on a channel
func (c *Consumer) handleErrors(ctx context.Context) {
	c.logger.Info("Starting error handler")

	errChan := c.internalConsumer.Errors()
	errMetrics := c.newErrMetrics()
	for err := range errChan {
		errMetrics.errCounter.Inc(1)
		c.logger.Error("Error consuming from Kafka", zap.Error(err))
	}
}
