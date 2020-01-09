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

package lib

import (
	"context"
	"github.com/SENERGY-Platform/process-incident-worker/lib/camunda"
	"github.com/SENERGY-Platform/process-incident-worker/lib/configuration"
	"github.com/SENERGY-Platform/process-incident-worker/lib/controller"
	"github.com/SENERGY-Platform/process-incident-worker/lib/database"
	"github.com/SENERGY-Platform/process-incident-worker/lib/interfaces"
	"github.com/SENERGY-Platform/process-incident-worker/lib/source"
	"log"
)

func Start(ctx context.Context, config configuration.Config) (err error) {
	return StartWith(ctx, config, source.Factory, camunda.Factory, database.Factory, func(err error) {
		log.Fatalf("FATAL: %+v", err)
	})
}

func StartWith(parentCtx context.Context, config configuration.Config, source interfaces.SourceFactory, camunda interfaces.CamundaFactory, database interfaces.DatabaseFactory, errorHandler func(err error)) (err error) {
	ctx, cancel := context.WithCancel(parentCtx)
	camundaInstance, err := camunda.Get(ctx, config)
	if err != nil {
		cancel()
		return err
	}
	databaseInstance, err := database.Get(ctx, config)
	if err != nil {
		cancel()
		return err
	}
	ctrl := controller.New(ctx, config, camundaInstance, databaseInstance)
	err = source.Start(ctx, config, ctrl, errorHandler)
	if err != nil {
		cancel()
		return err
	}
	return nil
}
