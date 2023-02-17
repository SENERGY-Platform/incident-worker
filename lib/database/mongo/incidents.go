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

func (this *Mongo) SaveIncident(incident messages.Incident) error {
	ctx, _ := context.WithTimeout(context.Background(), TIMEOUT)
	_, err := this.incidentsCollection().ReplaceOne(ctx, bson.M{"id": incident.Id}, incident, options.Replace().SetUpsert(true))
	return errors.WithStack(err)
}

func (this *Mongo) DeleteIncidentByInstanceId(id string) error {
	ctx, _ := context.WithTimeout(context.Background(), TIMEOUT)
	_, err := this.incidentsCollection().DeleteMany(ctx, bson.M{"process_instance_id": id})
	return errors.WithStack(err)
}

func (this *Mongo) DeleteIncidentByDefinitionId(id string) error {
	ctx, _ := context.WithTimeout(context.Background(), TIMEOUT)
	_, err := this.incidentsCollection().DeleteMany(ctx, bson.M{"process_definition_id": id})
	return errors.WithStack(err)
}

func (this *Mongo) incidentsCollection() *mongo.Collection {
	return this.client.Database(this.config.MongoDatabaseName).Collection(this.config.MongoIncidentCollectionName)
}
