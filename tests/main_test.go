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
	"github.com/SENERGY-Platform/incident-worker/lib"
	"github.com/SENERGY-Platform/incident-worker/lib/camunda"
	"github.com/SENERGY-Platform/incident-worker/lib/database"
	"github.com/SENERGY-Platform/incident-worker/lib/messages"
	"github.com/SENERGY-Platform/incident-worker/lib/source"
	"github.com/SENERGY-Platform/incident-worker/lib/util"
	"github.com/SENERGY-Platform/incident-worker/tests/server"
	"testing"
	"time"
)

func TestInit(t *testing.T) {
	defaultConfig, err := util.LoadConfig("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	defaultConfig.Debug = true

	ctx, cancel := context.WithCancel(context.Background())
	defer time.Sleep(10 * time.Second) //wait for docker cleanup
	defer cancel()

	config, err := server.New(ctx, defaultConfig)
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
}

func TestDatabase(t *testing.T) {
	defaultConfig, err := util.LoadConfig("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	defaultConfig.Debug = true

	ctx, cancel := context.WithCancel(context.Background())
	defer time.Sleep(10 * time.Second) //wait for docker cleanup
	defer cancel()

	config, err := server.New(ctx, defaultConfig)
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

	incident := messages.KafkaIncidentMessage{
		Id:                  "foo_id",
		MsgVersion:          1,
		ExternalTaskId:      "task_id",
		ProcessInstanceId:   "piid",
		ProcessDefinitionId: "pdid",
		WorkerId:            "w",
		ErrorMessage:        "error message",
		Time:                time.Now(),
	}

	t.Run("send incident", func(t *testing.T) {
		sendIncidentToKafka(t, config, incident)
	})

	time.Sleep(10 * time.Second)

	t.Run("check database", func(t *testing.T) {
		checkIncidentInDatabase(t, config, incident)
	})
}

func TestCamunda(t *testing.T) {
	defaultConfig, err := util.LoadConfig("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	defaultConfig.Debug = true

	ctx, cancel := context.WithCancel(context.Background())
	defer time.Sleep(10 * time.Second) //wait for docker cleanup
	defer cancel()

	config, err := server.New(ctx, defaultConfig)
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

	incident := messages.KafkaIncidentMessage{
		Id:                  "foo_id",
		MsgVersion:          1,
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

	t.Run("check database", func(t *testing.T) {
		checkIncidentInDatabase(t, config, incident)
	})

	t.Run("check process", func(t *testing.T) {
		checkProcess(t, config, instanceId, false)
	})

}
