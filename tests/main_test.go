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
	"encoding/json"
	"github.com/SENERGY-Platform/incident-worker/lib"
	"github.com/SENERGY-Platform/incident-worker/lib/camunda"
	"github.com/SENERGY-Platform/incident-worker/lib/database"
	"github.com/SENERGY-Platform/incident-worker/lib/messages"
	"github.com/SENERGY-Platform/incident-worker/lib/source"
	kutil "github.com/SENERGY-Platform/incident-worker/lib/source/util"
	"github.com/SENERGY-Platform/incident-worker/lib/util"
	"github.com/SENERGY-Platform/incident-worker/tests/server"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"reflect"
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

func checkIncidentInDatabase(t *testing.T, config util.Config, message messages.KafkaIncidentMessage) {
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.MongoUrl))
	if err != nil {
		err = errors.WithStack(err)
		t.Fatalf("ERROR: %+v", err)
		return
	}
	result := client.Database(config.MongoDatabaseName).Collection(config.MongoIncidentCollectionName).FindOne(ctx, bson.M{"id": message.Id})
	err = result.Err()
	if err != nil {
		err = errors.WithStack(err)
		t.Fatalf("ERROR: %+v", err)
		return
	}
	compare := messages.KafkaIncidentMessage{}
	err = result.Decode(&compare)
	if err != nil {
		err = errors.WithStack(err)
		t.Fatalf("ERROR: %+v", err)
		return
	}

	if message.Time.Unix() != compare.Time.Unix() {
		t.Fatal(message.Time.Unix(), compare.Time.Unix())
	}
	message.Time = time.Time{}
	compare.Time = time.Time{}
	if !reflect.DeepEqual(message, compare) {
		t.Fatal(message, compare)
	}
}

func sendIncidentToKafka(t *testing.T, config util.Config, cmd messages.KafkaIncidentMessage) {
	broker, err := kutil.GetBroker(config.ZookeeperUrl)
	if err != nil {
		err = errors.WithStack(err)
		t.Fatalf("ERROR: %+v", err)
		return
	}
	if len(broker) == 0 {
		t.Fatalf("ERROR: %+v", errors.New("missing kafka broker"))
		return
	}
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:     broker,
		Topic:       config.KafkaIncidentTopic,
		MaxAttempts: 10,
		Logger:      log.New(os.Stdout, "[KAFKA-PRODUCER] ", 0),
	})

	message, err := json.Marshal(cmd)
	if err != nil {
		err = errors.WithStack(err)
		t.Fatalf("ERROR: %+v", err)
		return
	}

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(cmd.Id),
		Value: message,
		Time:  time.Now(),
	})

	if err != nil {
		err = errors.WithStack(err)
		t.Fatalf("ERROR: %+v", err)
		return
	}

	err = writer.Close()
	if err != nil {
		err = errors.WithStack(err)
		t.Fatalf("ERROR: %+v", err)
		return
	}
}
