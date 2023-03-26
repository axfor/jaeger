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
	"strings"
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

	deadlockDetector deadlockDetector

	partitionsHeld      atomic.Int64
	partitionsHeldGauge metrics.Gauge

	doneWg         sync.WaitGroup
	ready          chan struct{}
	readyCloseLock sync.Mutex

	topic  string
	cancel context.CancelFunc
}

// New is a constructor for a Consumer
func New(params Params) (*Consumer, error) {
	deadlockDetector := newDeadlockDetector(params.MetricsFactory, params.Logger, params.DeadlockCheckInterval)
	return &Consumer{
		metricsFactory:      params.MetricsFactory,
		logger:              params.Logger,
		internalConsumer:    params.InternalConsumer,
		processorFactory:    params.ProcessorFactory,
		deadlockDetector:    deadlockDetector,
		partitionsHeldGauge: partitionsHeldGauge(params.MetricsFactory),
		ready:               make(chan struct{}),
		topic:               params.ProcessorFactory.topic,
	}, nil
}

// Start begins consuming messages in a go routine
func (c *Consumer) Start() {
	c.deadlockDetector.start()
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.doneWg.Add(1)

	go func() {
		defer c.doneWg.Done()
		for {
			select {
			case <-ctx.Done():
				c.logger.Error("ctx canceld")
				return
			default:
				c.logger.Info("Topic", zap.Strings("topic", strings.Split(c.topic, ",")))
				if err := c.internalConsumer.Consume(ctx, strings.Split(c.topic, ","), c); err != nil {
					c.logger.Error("Error from consumer", zap.Error(err))
				}
				// check if context was cancelled, signaling that the consumer should stop
				if ctx.Err() != nil {
					return
				}
				c.ready = make(chan struct{})
			}
		}
	}()
}

// Close closes the Consumer and underlying sarama consumer
func (c *Consumer) Close() error {
	// Close the internal consumer, which will close each partition consumers' message and error channels.
	c.logger.Info("Closing parent consumer")
	err := c.internalConsumer.Close()

	c.logger.Debug("Closing deadlock detector")
	c.deadlockDetector.close()
	if c.cancel != nil {
		c.cancel()
	}

	c.logger.Debug("Waiting for messages and errors to be handled")
	c.doneWg.Wait()

	return err
}

// Ready is consumer running
func (c *Consumer) Ready() {
	<-c.ready
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready

	c.readyCloseLock.Lock()
	defer c.readyCloseLock.Unlock()

	select {
	case <-c.ready:
	default:
		close(c.ready)
	}

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.partitionMetrics(claim.Partition()).startCounter.Inc(1)

	c.doneWg.Add(2)
	go c.handleErrors(claim.Partition(), c.internalConsumer.Errors())
	c.handleMessages(session, claim)
	return nil
}

// handleMessages starting message handler
func (c *Consumer) handleMessages(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) {
	c.logger.Info("Starting message handler", zap.Int32("partition", claim.Partition()))

	c.partitionsHeld.Add(1)
	c.partitionsHeldGauge.Update(c.partitionsHeld.Load())

	defer func() {
		c.closePartition(claim)
		c.partitionsHeld.Add(-1)
		c.partitionsHeldGauge.Update(c.partitionsHeld.Load())
		c.doneWg.Done()
	}()

	msgMetrics := c.newMsgMetrics(claim.Partition())

	var msgProcessor processor.SpanProcessor

	deadlockDetector := c.deadlockDetector.startMonitoringForPartition(claim.Partition())
	defer deadlockDetector.close()

	for {
		select {
		case msg := <-claim.Messages():

			c.logger.Debug("Got msg", zap.Any("msg", msg))
			msgMetrics.counter.Inc(1)
			msgMetrics.offsetGauge.Update(msg.Offset)
			msgMetrics.lagGauge.Update(claim.HighWaterMarkOffset() - msg.Offset - 1)
			deadlockDetector.incrementMsgCount()

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
			return
		case <-deadlockDetector.closePartitionChannel():
			c.logger.Info("Closing partition due to inactivity", zap.Int32("partition", claim.Partition()))
			return
		}
	}
}

// closePartition close partition of consumer
func (c *Consumer) closePartition(claim sarama.ConsumerGroupClaim) {
	c.logger.Info("Closing partition consumer", zap.Int32("partition", claim.Partition()))
	c.partitionMetrics(claim.Partition()).closeCounter.Inc(1)
}

// handleErrors handles incoming Kafka consumer errors on a channel
func (c *Consumer) handleErrors(partition int32, errChan <-chan error) {
	c.logger.Info("Starting error handler", zap.Int32("partition", partition))
	defer c.doneWg.Done()

	errMetrics := c.newErrMetrics(partition)
	for err := range errChan {
		errMetrics.errCounter.Inc(1)
		c.logger.Error("Error consuming from Kafka", zap.Error(err))
	}
	c.logger.Info("Finished handling errors", zap.Int32("partition", partition))
}
