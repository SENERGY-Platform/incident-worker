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
	"encoding/json"
	"fmt"
	developerNotifications "github.com/SENERGY-Platform/developer-notifications/pkg/client"
	"github.com/SENERGY-Platform/process-incident-worker/lib/configuration"
	"github.com/SENERGY-Platform/process-incident-worker/lib/interfaces"
	"github.com/SENERGY-Platform/process-incident-worker/lib/messages"
	"github.com/SENERGY-Platform/process-incident-worker/lib/notification"
	"github.com/SENERGY-Platform/service-commons/pkg/cache"
	"log"
	"log/slog"
	"os"
	"runtime/debug"
	"time"
)

type Controller struct {
	config                configuration.Config
	camunda               interfaces.Camunda
	db                    interfaces.Database
	metrics               Metric
	devNotifications      developerNotifications.Client
	logger                *slog.Logger
	handledIncidentsCache *cache.Cache
}

type Metric interface {
	NotifyIncidentMessage()
}

func New(ctx context.Context, config configuration.Config, camunda interfaces.Camunda, db interfaces.Database, m Metric) (ctrl *Controller, err error) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if info, ok := debug.ReadBuildInfo(); ok {
		logger = logger.With("go-module", info.Path)
	}
	c, err := cache.New(cache.Config{}) //if the worker is scaled, the l2 must be configured with a shared memcached
	if err != nil {
		return nil, err
	}
	ctrl = &Controller{config: config, camunda: camunda, db: db, metrics: m, logger: logger, handledIncidentsCache: c}
	if config.DeveloperNotificationUrl != "" && config.DeveloperNotificationUrl != "-" {
		ctrl.devNotifications = developerNotifications.New(config.DeveloperNotificationUrl)
	}
	return ctrl, nil
}

type MsgVersionWrapper struct {
	MsgVersion int64 `json:"msg_version"`
}

func getMsgVersion(msg []byte) (version int64, err error) {
	wrapper := MsgVersionWrapper{}
	err = json.Unmarshal(msg, &wrapper)
	return wrapper.MsgVersion, err
}

func (this *Controller) HandleIncidentMessage(msg []byte) error {
	version, err := getMsgVersion(msg)
	if err != nil {
		this.logger.Error("unable to parse msg -> ignore", "snrgy-log-type", "error", "error", err.Error(), "msg", string(msg))
		return nil
	}
	if version == 1 || version == 2 {
		incident := messages.Incident{}
		err = json.Unmarshal(msg, &incident)
		if err != nil {
			this.logger.Error("unable to parse msg -> ignore", "snrgy-log-type", "error", "error", err.Error(), "msg", string(msg))
			return nil
		}
		err = this.CreateIncident(incident)
		if err != nil {
			this.logger.Error("unable to hande incident create", "snrgy-log-type", "error", "error", err.Error(), "user", incident.TenantId, "process-definition-id", incident.ProcessDefinitionId, "incident-msg", incident.ErrorMessage)
		}
		return err
	}
	if version == 3 {
		command := messages.KafkaIncidentsCommand{}
		err = json.Unmarshal(msg, &command)
		if err != nil {
			this.logger.Error("unable to parse msg -> ignore", "snrgy-log-type", "error", "error", err.Error(), "msg", string(msg))
			return nil
		}
		if command.Command == "PUT" || command.Command == "POST" {
			if command.Incident != nil {
				command.Incident.MsgVersion = command.MsgVersion
				err = this.CreateIncident(*command.Incident)
				if err != nil {
					this.logger.Error("unable to hande incident PUT/POST", "snrgy-log-type", "error", "error", err.Error(), "user", command.Incident.TenantId, "process-definition-id", command.Incident.ProcessDefinitionId, "incident-msg", command.Incident.ErrorMessage)
				}
				return err
			}
		}
		if command.Command == "DELETE" {
			if command.ProcessDefinitionId != "" {
				err = this.DeleteIncidentByProcessDefinitionId(command.ProcessDefinitionId)
				if err != nil {
					this.logger.Error("unable to hande incident DELETE", "snrgy-log-type", "error", "error", err.Error(), "process-definition-id", command.ProcessDefinitionId)
				}
				return err
			}
			if command.ProcessInstanceId != "" {
				err = this.DeleteIncidentByProcessInstanceId(command.ProcessInstanceId)
				if err != nil {
					this.logger.Error("unable to hande incident DELETE", "snrgy-log-type", "error", "error", err.Error(), "process-instance-id", command.ProcessInstanceId)
				}
				return err
			}
		}
		if command.Command == "HANDLER" && command.Handler != nil {
			err = this.SetOnIncidentHandler(*command.Handler)
			if err != nil {
				this.logger.Error("unable to hande incident HANDLER", "snrgy-log-type", "error", "error", err.Error(), "process-definition-id", command.Handler.ProcessDefinitionId)
			}
			return err
		}
	}
	return nil
}

func (this *Controller) CreateIncident(incident messages.Incident) (err error) {
	//for every process instance an incident may only be handled once every 5 min
	//use the cache.Use method to do incident handling, only if the process instance is not found in cache
	//incident.ProcessInstanceId should be enough as key but existing tests would fail, so the incident.ProcessDefinitionId is added
	_, err = cache.Use[string](this.handledIncidentsCache, incident.ProcessDefinitionId+"+"+incident.ProcessInstanceId, func() (string, error) {
		return "", this.createIncident(incident)
	}, cache.NoValidation, 5*time.Minute)
	return err
}

func (this *Controller) createIncident(incident messages.Incident) (err error) {
	this.metrics.NotifyIncidentMessage()
	handling, registeredHandling, err := this.db.GetOnIncident(incident.ProcessDefinitionId)
	if err != nil {
		log.Println("ERROR: ", err)
		debug.PrintStack()
		return err
	}
	name, err := this.camunda.GetProcessName(incident.ProcessDefinitionId, incident.TenantId)
	if err != nil {
		this.logger.Error("unable to get process name", "snrgy-log-type", "warning", "error", err.Error())
		incident.DeploymentName = incident.ProcessDefinitionId
	} else {
		incident.DeploymentName = name
	}
	this.logger.Info("process-incident", "snrgy-log-type", "process-incident", "error", incident.ErrorMessage, "user", incident.TenantId, "deployment-name", incident.DeploymentName, "process-definition-id", incident.ProcessDefinitionId)
	if incident.TenantId != "" {
		if !registeredHandling || handling.Notify {
			msg := notification.Message{
				UserId:  incident.TenantId,
				Title:   "Process-Incident in " + incident.DeploymentName,
				Message: incident.ErrorMessage,
			}
			if registeredHandling && handling.Restart {
				msg.Message = msg.Message + "\n\nprocess will be restarted"
			}
			this.Notify(msg)
		}
	}
	err = this.camunda.StopProcessInstance(incident.ProcessInstanceId, incident.TenantId)
	if err != nil {
		return err
	}
	err = this.db.SaveIncident(incident)
	if err != nil {
		return err
	}
	if registeredHandling && handling.Restart {
		err = this.camunda.StartProcess(incident.ProcessDefinitionId, incident.TenantId)
		if err != nil {
			this.logger.Error("unable to restart process", "snrgy-log-type", "process-incident", "error", err.Error(), "user", incident.TenantId, "deployment-name", incident.DeploymentName, "process-definition-id", incident.ProcessDefinitionId)
			if incident.TenantId != "" {
				this.Notify(notification.Message{
					UserId:  incident.TenantId,
					Title:   "ERROR: unable to restart process after incident in: " + incident.DeploymentName,
					Message: fmt.Sprintf("Restart-Error: %v \n\n Incident: %v \n", err, incident.ErrorMessage),
				})
			}
		}
	}
	return nil
}

func (this *Controller) DeleteIncidentByProcessInstanceId(id string) error {
	return this.db.DeleteIncidentByInstanceId(id)
}

func (this *Controller) DeleteIncidentByProcessDefinitionId(id string) error {
	return this.db.DeleteByDefinitionId(id)
}

func (this *Controller) SetOnIncidentHandler(handler messages.OnIncident) error {
	return this.db.SaveOnIncident(handler)
}

func (this *Controller) Notify(msg notification.Message) {
	_ = notification.Send(this.config.NotificationUrl, msg)
	if this.devNotifications != nil {
		go func() {
			if this.config.Debug {
				log.Println("DEBUG: send developer-notification")
			}
			err := this.devNotifications.SendMessage(developerNotifications.Message{
				Sender: "github.com/SENERGY-Platform/process-incident-worker",
				Title:  "Process-Incident-User-Notification",
				Tags:   []string{"process-incident", "user-notification", msg.UserId},
				Body:   fmt.Sprintf("Notification For %v\nTitle: %v\nMessage: %v\n", msg.UserId, msg.Title, msg.Message),
			})
			if err != nil {
				log.Println("ERROR: unable to send developer-notification", err)
			}
		}()
	}
}
