/*
 * Copyright 2019 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package consumer

import (
	"context"
	"github.com/SENERGY-Platform/process-incident-worker/lib/configuration"
	"github.com/SENERGY-Platform/process-incident-worker/lib/interfaces"
	"github.com/SENERGY-Platform/process-incident-worker/lib/source/consumer/listener"
	"log"
)

func Start(ctx context.Context, config configuration.Config, control interfaces.Controller, runtimeErrorHandler func(err error)) (err error) {
	for _, factory := range listener.Factories {
		topic, handler, err := factory(config, control)
		if err != nil {
			return err
		}
		err = RunConsumer(ctx, config.ZookeeperUrl, config.KafkaConsumerGroup, topic, config.Debug, config.TopicConfigMap, func(topic string, msg []byte) error {
			if config.Debug {
				log.Println("DEBUG: consume", topic, string(msg))
			}
			return handler(msg)
		}, runtimeErrorHandler)
		if err != nil {
			return err
		}
	}
	return err
}
