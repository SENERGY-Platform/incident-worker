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
	"github.com/SENERGY-Platform/process-incident-worker/lib/configuration"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

const TIMEOUT = 10 * time.Second

type Mongo struct {
	config configuration.Config
	client *mongo.Client
	ctx    context.Context
}

func New(ctx context.Context, config configuration.Config) (result *Mongo, err error) {
	result = &Mongo{config: config, ctx: ctx}
	timeout, _ := context.WithTimeout(ctx, TIMEOUT)
	result.client, err = mongo.Connect(timeout, options.Client().ApplyURI(config.MongoUrl))
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}
	go func() {
		<-ctx.Done()
		log.Println("disconnect mongodb")
		disconnectCtx, _ := context.WithTimeout(context.Background(), TIMEOUT)
		result.client.Disconnect(disconnectCtx)
	}()
	return result, result.initIndexes()
}

func (this *Mongo) getTimeoutContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(this.ctx, TIMEOUT)
}

func (this *Mongo) initIndexes() error {
	// incident indexes are created by github.com/SENERGY-Platform/process-incident-api

	// on-incident indexes
	err := this.ensureIndex(this.onIncidentsCollection(), "on_incident_process_definition_id_index", OnIncidentBson.ProcessDefinitionId, true, false)
	if err != nil {
		return err
	}
	return nil
}

func (this *Mongo) DeleteByDefinitionId(id string) error {
	err := this.DeleteIncidentByDefinitionId(id)
	if err != nil {
		return err
	}
	return this.DeleteOnIncidentByDefinitionId(id)
}
