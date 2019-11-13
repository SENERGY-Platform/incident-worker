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
	"github.com/SENERGY-Platform/incident-worker/lib/configuration"
	"github.com/SENERGY-Platform/incident-worker/lib/interfaces"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

type FactoryType struct{}

var Factory = &FactoryType{}

type Camunda struct {
	config configuration.Config
}

func (this *FactoryType) Get(ctx context.Context, config configuration.Config) (interfaces.Camunda, error) {
	return &Camunda{config: config}, nil
}

func (this *Camunda) StopProcessInstance(id string) (err error) {
	client := &http.Client{Timeout: 5 * time.Second}
	request, err := http.NewRequest("DELETE", this.config.CamundaUrl+"/engine-rest/process-instance/"+url.PathEscape(id)+"?skipIoMappings=true", nil)
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
	err = errors.New("error on delete in engine for " + this.config.CamundaUrl + "/engine-rest/process-instance/" + url.PathEscape(id) + ": " + resp.Status + " " + string(msg))
	return err
}
