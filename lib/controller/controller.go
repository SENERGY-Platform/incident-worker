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

package controller

import (
	"context"
	"github.com/SENERGY-Platform/process-incident-worker/lib/configuration"
	"github.com/SENERGY-Platform/process-incident-worker/lib/interfaces"
	"github.com/SENERGY-Platform/process-incident-worker/lib/messages"
)

type Controller struct {
	config  configuration.Config
	camunda interfaces.Camunda
	db      interfaces.Database
}

func New(ctx context.Context, config configuration.Config, camunda interfaces.Camunda, db interfaces.Database) *Controller {
	return &Controller{config: config, camunda: camunda, db: db}
}

func (this *Controller) HandleIncident(incident messages.KafkaIncidentMessage) error {
	if incident.MsgVersion != 1 && incident.MsgVersion != 2 {
		return nil
	}
	err := this.camunda.StopProcessInstance(incident.ProcessInstanceId)
	if err != nil {
		return err
	}
	return this.db.Save(incident)
}
