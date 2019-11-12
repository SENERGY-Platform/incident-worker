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
	"github.com/SENERGY-Platform/incident-worker/lib/messages"
	"github.com/SENERGY-Platform/incident-worker/lib/util"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"testing"
	"time"
)

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
