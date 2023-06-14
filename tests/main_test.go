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

package tests

import (
	"context"
	"github.com/SENERGY-Platform/process-incident-worker/lib"
	"github.com/SENERGY-Platform/process-incident-worker/lib/camunda"
	"github.com/SENERGY-Platform/process-incident-worker/lib/configuration"
	"github.com/SENERGY-Platform/process-incident-worker/lib/database"
	"github.com/SENERGY-Platform/process-incident-worker/lib/messages"
	"github.com/SENERGY-Platform/process-incident-worker/lib/source"
	"github.com/SENERGY-Platform/process-incident-worker/tests/server"
	"sync"
	"testing"
	"time"
)

func TestDatabaseDeprecated(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defaultConfig, err := configuration.LoadConfig("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	defaultConfig.Debug = true

	config, err := server.New(ctx, wg, defaultConfig)
	if err != nil {
		t.Error(err)
		return
	}

	err = lib.StartWith(ctx, config, source.Factory, camunda.Factory, database.Factory, func(err error) {
		t.Errorf("ERROR: %+v", err)
	})
	if err != nil {
		t.Error(err)
		return
	}

	incident := messages.Incident{
		Id:                  "foo_id",
		MsgVersion:          2,
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid",
		ProcessDefinitionId: "pdid",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Now(),
		DeploymentName:      "pdid",
	}

	t.Run("send incident", func(t *testing.T) {
		sendIncidentToKafka(t, config, incident)
	})

	time.Sleep(10 * time.Second)

	t.Run("check database", func(t *testing.T) {
		checkIncidentInDatabase(t, config, incident)
	})
}

func TestDatabase(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defaultConfig, err := configuration.LoadConfig("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	defaultConfig.Debug = true

	config, err := server.New(ctx, wg, defaultConfig)
	if err != nil {
		t.Error(err)
		return
	}

	err = lib.StartWith(ctx, config, source.Factory, camunda.Factory, database.Factory, func(err error) {
		t.Errorf("ERROR: %+v", err)
	})
	if err != nil {
		t.Error(err)
		return
	}

	incident := messages.Incident{
		Id:                  "foo_id",
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid",
		ProcessDefinitionId: "pdid",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Now(),
		DeploymentName:      "pdid",
	}

	t.Run("send incident", func(t *testing.T) {
		sendIncidentV3ToKafka(t, config, incident)
	})

	time.Sleep(10 * time.Second)

	t.Run("check database", func(t *testing.T) {
		incident.MsgVersion = 3
		checkIncidentInDatabase(t, config, incident)
	})
}

func TestCamunda(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defaultConfig, err := configuration.LoadConfig("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	defaultConfig.Debug = true

	config, err := server.New(ctx, wg, defaultConfig)
	if err != nil {
		t.Error(err)
		return
	}

	err = lib.StartWith(ctx, config, source.Factory, camunda.Factory, database.Factory, func(err error) {
		t.Errorf("ERROR: %+v", err)
	})
	if err != nil {
		t.Error(err)
		return
	}

	definitionId := ""
	t.Run("deploy process", func(t *testing.T) {
		definitionId = deployProcess(t, config)
	})

	time.Sleep(10 * time.Second)

	instanceId := ""
	t.Run("start process", func(t *testing.T) {
		instanceId = startProcess(t, config, definitionId)
	})

	t.Run("check process", func(t *testing.T) {
		checkProcess(t, config, instanceId, true)
	})

	incident := messages.Incident{
		Id:                  "foo_id",
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   instanceId,
		ProcessDefinitionId: definitionId,
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Now(),
	}

	t.Run("send incident", func(t *testing.T) {
		sendIncidentV3ToKafka(t, config, incident)
	})

	time.Sleep(10 * time.Second)

	incident.DeploymentName = "test"
	t.Run("check database", func(t *testing.T) {
		incident.MsgVersion = 3
		checkIncidentInDatabase(t, config, incident)
	})

	t.Run("check process", func(t *testing.T) {
		checkProcess(t, config, instanceId, false)
	})
}

func TestCamundaDeprecated(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defaultConfig, err := configuration.LoadConfig("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	defaultConfig.Debug = true

	config, err := server.New(ctx, wg, defaultConfig)
	if err != nil {
		t.Error(err)
		return
	}

	err = lib.StartWith(ctx, config, source.Factory, camunda.Factory, database.Factory, func(err error) {
		t.Errorf("ERROR: %+v", err)
	})
	if err != nil {
		t.Error(err)
		return
	}

	definitionId := ""
	t.Run("deploy process", func(t *testing.T) {
		definitionId = deployProcess(t, config)
	})

	time.Sleep(10 * time.Second)

	instanceId := ""
	t.Run("start process", func(t *testing.T) {
		instanceId = startProcess(t, config, definitionId)
	})

	t.Run("check process", func(t *testing.T) {
		checkProcess(t, config, instanceId, true)
	})

	incident := messages.Incident{
		Id:                  "foo_id",
		MsgVersion:          2,
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   instanceId,
		ProcessDefinitionId: definitionId,
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Now(),
	}

	t.Run("send incident", func(t *testing.T) {
		sendIncidentToKafka(t, config, incident)
	})

	time.Sleep(10 * time.Second)

	incident.DeploymentName = "test"
	t.Run("check database", func(t *testing.T) {
		checkIncidentInDatabase(t, config, incident)
	})

	t.Run("check process", func(t *testing.T) {
		checkProcess(t, config, instanceId, false)
	})
}

func TestDeleteByDeploymentIdDeprecated(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defaultConfig, err := configuration.LoadConfig("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	defaultConfig.Debug = true

	config, err := server.New(ctx, wg, defaultConfig)
	if err != nil {
		t.Error(err)
		return
	}

	err = lib.StartWith(ctx, config, source.Factory, camunda.Factory, database.Factory, func(err error) {
		t.Errorf("ERROR: %+v", err)
	})
	if err != nil {
		t.Error(err)
		return
	}

	incident11 := messages.Incident{
		Id:                  "a",
		MsgVersion:          2,
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid1",
		ProcessDefinitionId: "pdid1",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid1",
	}
	incident12 := messages.Incident{
		Id:                  "b",
		MsgVersion:          2,
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid1",
		ProcessDefinitionId: "pdid2",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid2",
	}
	incident21 := messages.Incident{
		Id:                  "c",
		MsgVersion:          2,
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid2",
		ProcessDefinitionId: "pdid1",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid1",
	}
	incident22 := messages.Incident{
		Id:                  "d",
		MsgVersion:          2,
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid2",
		ProcessDefinitionId: "pdid2",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid2",
	}

	t.Run("send incidents", func(t *testing.T) {
		sendIncidentToKafka(t, config, incident11)
		sendIncidentToKafka(t, config, incident12)
		sendIncidentToKafka(t, config, incident21)
		sendIncidentToKafka(t, config, incident22)
	})

	time.Sleep(10 * time.Second)

	t.Run("send delete by deplymentId", func(t *testing.T) {
		sendDefinitionDeleteToKafka(t, config, "pdid1")
	})

	time.Sleep(10 * time.Second)

	t.Run("check database", func(t *testing.T) {
		checkIncidentsInDatabase(t, config, incident12, incident22)
	})
}

func TestDeleteByDeploymentId(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defaultConfig, err := configuration.LoadConfig("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	defaultConfig.Debug = true

	config, err := server.New(ctx, wg, defaultConfig)
	if err != nil {
		t.Error(err)
		return
	}

	err = lib.StartWith(ctx, config, source.Factory, camunda.Factory, database.Factory, func(err error) {
		t.Errorf("ERROR: %+v", err)
	})
	if err != nil {
		t.Error(err)
		return
	}

	t.Run("send incidents handler", func(t *testing.T) {
		sendIncidentHandler(t, config, "pdid1")
		sendIncidentHandler(t, config, "pdid2")
	})

	incident11 := messages.Incident{
		Id:                  "a",
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid1",
		ProcessDefinitionId: "pdid1",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid1",
	}
	incident12 := messages.Incident{
		Id:                  "b",
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid1",
		ProcessDefinitionId: "pdid2",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid2",
	}
	incident21 := messages.Incident{
		Id:                  "c",
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid2",
		ProcessDefinitionId: "pdid1",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid1",
	}
	incident22 := messages.Incident{
		Id:                  "d",
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid2",
		ProcessDefinitionId: "pdid2",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid2",
	}

	t.Run("send incidents", func(t *testing.T) {
		sendIncidentV3ToKafka(t, config, incident11)
		sendIncidentV3ToKafka(t, config, incident12)
		sendIncidentV3ToKafka(t, config, incident21)
		sendIncidentV3ToKafka(t, config, incident22)
	})

	time.Sleep(10 * time.Second)

	t.Run("send delete by deplymentId", func(t *testing.T) {
		sendDefinitionDeleteToKafka(t, config, "pdid1")
	})

	time.Sleep(10 * time.Second)

	t.Run("check database", func(t *testing.T) {
		incident11.MsgVersion = 3
		incident12.MsgVersion = 3
		incident21.MsgVersion = 3
		incident22.MsgVersion = 3
		checkIncidentsInDatabase(t, config, incident12, incident22)
	})

	t.Run("check on incidents handler in database", func(t *testing.T) {
		checkOnIncidentsInDatabase(t, config, messages.OnIncident{
			ProcessDefinitionId: "pdid2",
			Restart:             false,
			Notify:              false,
		})
	})
}

func TestDeleteByInstanceIdDeprecated(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defaultConfig, err := configuration.LoadConfig("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	defaultConfig.Debug = true

	config, err := server.New(ctx, wg, defaultConfig)
	if err != nil {
		t.Error(err)
		return
	}

	err = lib.StartWith(ctx, config, source.Factory, camunda.Factory, database.Factory, func(err error) {
		t.Errorf("ERROR: %+v", err)
	})
	if err != nil {
		t.Error(err)
		return
	}

	incident11 := messages.Incident{
		Id:                  "a",
		MsgVersion:          2,
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid1",
		ProcessDefinitionId: "pdid1",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid1",
	}
	incident12 := messages.Incident{
		Id:                  "b",
		MsgVersion:          2,
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid1",
		ProcessDefinitionId: "pdid2",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid2",
	}
	incident21 := messages.Incident{
		Id:                  "c",
		MsgVersion:          2,
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid2",
		ProcessDefinitionId: "pdid1",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid1",
	}
	incident22 := messages.Incident{
		Id:                  "d",
		MsgVersion:          2,
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid2",
		ProcessDefinitionId: "pdid2",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid2",
	}

	t.Run("send incidents", func(t *testing.T) {
		sendIncidentToKafka(t, config, incident11)
		sendIncidentToKafka(t, config, incident12)
		sendIncidentToKafka(t, config, incident21)
		sendIncidentToKafka(t, config, incident22)
	})

	time.Sleep(10 * time.Second)

	t.Run("send delete by instance", func(t *testing.T) {
		sendInstanceDeleteToKafka(t, config, "piid1")
	})

	time.Sleep(10 * time.Second)

	t.Run("check database", func(t *testing.T) {
		checkIncidentsInDatabase(t, config, incident21, incident22)
	})
}

func TestDeleteByInstanceId(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defaultConfig, err := configuration.LoadConfig("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	defaultConfig.Debug = true

	config, err := server.New(ctx, wg, defaultConfig)
	if err != nil {
		t.Error(err)
		return
	}

	err = lib.StartWith(ctx, config, source.Factory, camunda.Factory, database.Factory, func(err error) {
		t.Errorf("ERROR: %+v", err)
	})
	if err != nil {
		t.Error(err)
		return
	}

	incident11 := messages.Incident{
		Id:                  "a",
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid1",
		ProcessDefinitionId: "pdid1",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid1",
	}
	incident12 := messages.Incident{
		Id:                  "b",
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid1",
		ProcessDefinitionId: "pdid2",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid2",
	}
	incident21 := messages.Incident{
		Id:                  "c",
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid2",
		ProcessDefinitionId: "pdid1",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid1",
	}
	incident22 := messages.Incident{
		Id:                  "d",
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid2",
		ProcessDefinitionId: "pdid2",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Time{},
		DeploymentName:      "pdid2",
	}

	t.Run("send incidents", func(t *testing.T) {
		sendIncidentV3ToKafka(t, config, incident11)
		sendIncidentV3ToKafka(t, config, incident12)
		sendIncidentV3ToKafka(t, config, incident21)
		sendIncidentV3ToKafka(t, config, incident22)
	})

	time.Sleep(10 * time.Second)

	t.Run("send delete by instance", func(t *testing.T) {
		sendInstanceDeleteToKafka(t, config, "piid1")
	})

	time.Sleep(10 * time.Second)

	t.Run("check database", func(t *testing.T) {
		incident11.MsgVersion = 3
		incident12.MsgVersion = 3
		incident21.MsgVersion = 3
		incident22.MsgVersion = 3
		checkIncidentsInDatabase(t, config, incident21, incident22)
	})
}
