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

package listener

import (
	"encoding/json"
	"github.com/SENERGY-Platform/process-incident-worker/lib/configuration"
	"github.com/SENERGY-Platform/process-incident-worker/lib/interfaces"
	"github.com/SENERGY-Platform/process-incident-worker/lib/messages"
	"github.com/pkg/errors"
	"log"
)

func init() {
	Factories = append(Factories, ProcessInstanceEventListenerFactory)
}

func ProcessInstanceEventListenerFactory(config configuration.Config, control interfaces.Controller) (topic string, listener Listener, err error) {
	return config.KafkaProcessInstanceHistoryEventTopic, func(msg []byte) (err error) {
		defer func() {
			if err != nil {
				log.Printf("ERROR: %+v \n", err)
			}
		}()
		message := messages.ProcessInstanceHistoryEvent{}
		err = json.Unmarshal(msg, &message)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		err = control.HandleProcessInstanceHistoryEvent(message)
		return
	}, nil
}
