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

package interfaces

import (
	"context"
	"github.com/SENERGY-Platform/incident-worker/lib/configuration"
	"github.com/SENERGY-Platform/incident-worker/lib/messages"
)

type Controller interface {
	HandleIncident(incident messages.KafkaIncidentMessage) error
}

type Camunda interface {
	StopProcessInstance(id string) (err error)
}

type CamundaFactory interface {
	Get(ctx context.Context, config configuration.Config) (Camunda, error)
}

type Database interface {
	Save(incident messages.KafkaIncidentMessage) error
}

type DatabaseFactory interface {
	Get(ctx context.Context, config configuration.Config) (Database, error)
}

type SourceFactory interface {
	Start(ctx context.Context, config configuration.Config, control Controller, runtimeErrorHandler func(err error)) (err error)
}
