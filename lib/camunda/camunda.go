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

package camunda

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/process-incident-worker/lib/camunda/cache"
	"github.com/SENERGY-Platform/process-incident-worker/lib/camunda/shards"
	"github.com/SENERGY-Platform/process-incident-worker/lib/configuration"
	"github.com/SENERGY-Platform/process-incident-worker/lib/interfaces"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"time"
)

type FactoryType struct{}

var Factory = &FactoryType{}

type Camunda struct {
	config configuration.Config
	shards *shards.Shards
}

func (this *FactoryType) Get(ctx context.Context, config configuration.Config) (interfaces.Camunda, error) {
	s, err := shards.New(config.ShardsDb, cache.New(&cache.CacheConfig{L1Expiration: 60}))
	if err != nil {
		return nil, err
	}
	return &Camunda{config: config, shards: s}, nil
}

func (this *Camunda) StopProcessInstance(id string, tenantId string) (err error) {
	shard, err := this.shards.GetShardForUser(tenantId)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	request, err := http.NewRequest("DELETE", shard+"/engine-rest/process-instance/"+url.PathEscape(id)+"?skipIoMappings=true", nil)
	if err != nil {
		return errors.WithStack(err)
	}
	resp, err := client.Do(request)
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode == 200 || resp.StatusCode == 204 {
		return nil
	}
	msg, _ := ioutil.ReadAll(resp.Body)
	err = errors.New("error on delete in engine for " + shard + "/engine-rest/process-instance/" + url.PathEscape(id) + ": " + resp.Status + " " + string(msg))
	return err
}

type NameWrapper struct {
	Name string `json:"name"`
}

func (this *Camunda) GetProcessName(id string, tenantId string) (name string, err error) {
	shard, err := this.shards.GetShardForUser(tenantId)
	if err != nil {
		return "", errors.WithStack(err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	request, err := http.NewRequest("GET", shard+"/engine-rest/process-definition/"+url.PathEscape(id), nil)
	if err != nil {
		return "", errors.WithStack(err)
	}
	resp, err := client.Do(request)
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		temp, _ := ioutil.ReadAll(resp.Body)
		log.Println("ERROR:", resp.Status, string(temp))
		debug.PrintStack()
		return "", errors.New("unexpected response")
	}
	result := NameWrapper{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	return result.Name, errors.WithStack(err)
}
