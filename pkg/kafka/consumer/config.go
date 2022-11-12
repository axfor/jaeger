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
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/pkg/kafka/auth"
)

// Consumer is an interface to features of Sarama that are necessary for the consumer
type Consumer interface {
	sarama.ConsumerGroup
}

// Builder builds a new kafka consumer
type Builder interface {
	NewConsumer() (Consumer, error)
}

// Configuration describes the configuration properties needed to create a Kafka consumer
type Configuration struct {
	auth.AuthenticationConfig `mapstructure:"authentication"`
	Consumer

	Brokers         []string `mapstructure:"brokers"`
	Topic           string   `mapstructure:"topic"`
	GroupID         string   `mapstructure:"group_id"`
	ClientID        string   `mapstructure:"client_id"`
	ProtocolVersion string   `mapstructure:"protocol_version"`
	RackID          string   `mapstructure:"rack_id"`
}

// NewConsumer creates a new kafka consumer
func (c *Configuration) NewConsumer(logger *zap.Logger) (Consumer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = c.ClientID
	saramaConfig.RackID = c.RackID
	if len(c.ProtocolVersion) > 0 {
		ver, err := sarama.ParseKafkaVersion(c.ProtocolVersion)
		if err != nil {
			return nil, err
		}
		saramaConfig.Version = ver
	}
	if err := c.AuthenticationConfig.SetConfiguration(saramaConfig, logger); err != nil {
		return nil, err
	}
	// cluster.NewConfig() uses sarama.NewConfig() to create the config.
	// However the Jaeger OTEL module pulls in newer samara version (from OTEL collector)
	// that does not set saramaConfig.Consumer.Offsets.CommitInterval to its default value 1s.
	// then the samara-cluster fails if the default interval is not 1s.
	saramaConfig.Consumer.Offsets.CommitInterval = time.Second

	client, err := sarama.NewConsumerGroup(c.Brokers, c.GroupID, saramaConfig)
	if err != nil {
		logger.Panic("error creating consumer group client", zap.Error(err))
	}
	return client, err
}
