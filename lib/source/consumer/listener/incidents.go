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
	"github.com/SENERGY-Platform/process-incident-worker/lib/configuration"
	"github.com/SENERGY-Platform/process-incident-worker/lib/interfaces"
	"log"
)

func init() {
	Factories = append(Factories, IncidentListenerFactory)
}

func IncidentListenerFactory(config configuration.Config, control interfaces.Controller) (topic string, listener Listener, err error) {
	return config.KafkaIncidentTopic, func(msg []byte) (err error) {
		defer func() {
			if err != nil {
				log.Printf("ERROR: %+v \n", err)
			}
		}()
		err = control.HandleIncidentMessage(msg)
		return
	}, nil
}
