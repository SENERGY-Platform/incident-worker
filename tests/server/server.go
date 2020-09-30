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

package server

import (
	"context"
	"github.com/SENERGY-Platform/process-incident-worker/lib/camunda/cache"
	"github.com/SENERGY-Platform/process-incident-worker/lib/camunda/shards"
	"github.com/SENERGY-Platform/process-incident-worker/lib/configuration"
	"github.com/SENERGY-Platform/process-incident-worker/tests/docker"
	"github.com/ory/dockertest"
	"log"
	"runtime/debug"
)

func New(parentCtx context.Context, init configuration.Config) (config configuration.Config, err error) {
	config = init

	ctx, cancel := context.WithCancel(parentCtx)

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	_, zk, err := docker.Zookeeper(pool, ctx)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}
	config.ZookeeperUrl = zk + ":2181"

	err = docker.Kafka(pool, ctx, config.ZookeeperUrl)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	_, pgIp, _, err := docker.Postgres(pool, ctx, nil, "camunda")
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	_, camundaIp, err := docker.Camunda(pool, ctx, pgIp, "5432")
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	_, _, shardsDb, err := docker.Postgres(pool, ctx, nil, "shards")
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}
	config.ShardsDb = shardsDb

	s, err := shards.New(shardsDb, cache.None)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	err = s.EnsureShard("http://" + camundaIp + ":8080")
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	_, err = s.EnsureShardForUser("")
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	_, ip, err := docker.Mongo(pool, ctx)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}
	config.MongoUrl = "mongodb://" + ip + ":27017"

	return config, nil
}
