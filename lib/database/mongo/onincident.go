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

package mongo

import (
	"context"
	"github.com/SENERGY-Platform/process-incident-worker/lib/messages"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var OnIncidentBson = getBsonFieldObject[messages.OnIncident]()

func (this *Mongo) SaveOnIncident(handler messages.OnIncident) error {
	ctx, _ := context.WithTimeout(context.Background(), TIMEOUT)
	_, err := this.onIncidentsCollection().ReplaceOne(ctx, bson.M{OnIncidentBson.ProcessDefinitionId: handler.ProcessDefinitionId}, handler, options.Replace().SetUpsert(true))
	return errors.WithStack(err)
}

func (this *Mongo) DeleteOnIncidentByDefinitionId(definitionId string) error {
	ctx, _ := context.WithTimeout(context.Background(), TIMEOUT)
	_, err := this.onIncidentsCollection().DeleteMany(ctx, bson.M{OnIncidentBson.ProcessDefinitionId: definitionId})
	return errors.WithStack(err)
}

func (this *Mongo) onIncidentsCollection() *mongo.Collection {
	return this.client.Database(this.config.MongoDatabaseName).Collection(this.config.MongoOnIncidentCollectionName)
}

func (this *Mongo) GetOnIncident(definitionId string) (handler messages.OnIncident, exists bool, err error) {
	ctx, _ := context.WithTimeout(context.Background(), TIMEOUT)
	result := this.onIncidentsCollection().FindOne(ctx, bson.M{OnIncidentBson.ProcessDefinitionId: definitionId})
	if err == mongo.ErrNoDocuments {
		return handler, false, nil
	}
	if err != nil {
		return handler, exists, err
	}
	err = result.Decode(&handler)
	if err == mongo.ErrNoDocuments {
		return handler, false, nil
	}
	return handler, true, err
}
