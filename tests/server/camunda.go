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
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest"
	"log"
	"net/http"
)

func Camunda(pool *dockertest.Pool, ctx context.Context, pgIp string, pgPort string) (hostPort string, ipAddress string, err error) {
	log.Println("start connectionlog")
	camunda, err := pool.Run("fgseitsrancher.wifa.intern.uni-leipzig.de:5000/process-engine", "dev", []string{
		"DB_PASSWORD=pw",
		"DB_URL=jdbc:postgresql://" + pgIp + ":" + pgPort + "/camunda",
		"DB_PORT=" + pgPort,
		"DB_NAME=camunda",
		"DB_HOST=" + pgIp,
		"DB_DRIVER=org.postgresql.Driver",
		"DB_USERNAME=usr",
		"DATABASE=postgres",
	})
	if err != nil {
		return "", "", err
	}
	hostPort = camunda.GetPort("8080/tcp")
	go func() {
		<-ctx.Done()
		camunda.Close()
	}()
	go Dockerlog(pool, ctx, camunda, "CAMUNDA")
	hostPort = camunda.GetPort("8080/tcp")
	err = pool.Retry(func() error {
		log.Println("try camunda connection...")
		resp, err := http.Get("http://localhost:" + hostPort + "/engine-rest/metrics")
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			log.Println("unexpectet response code", resp.StatusCode, resp.Status)
			return errors.New("unexpectet response code: " + resp.Status)
		}
		return nil
	})
	return hostPort, camunda.Container.NetworkSettings.IPAddress, err
}

func Postgres(pool *dockertest.Pool, ctx context.Context, dbName string) (hostPort string, ipAddress string, pgStr string, err error) {
	log.Println("start postgres")
	container, err := pool.Run("postgres", "latest", []string{
		"POSTGRES_DB=" + dbName, "POSTGRES_PASSWORD=pw", "POSTGRES_USER=usr",
	})
	if err != nil {
		return "", "", "", err
	}
	hostPort = container.GetPort("5432/tcp")
	go func() {
		<-ctx.Done()
		container.Close()
	}()
	go Dockerlog(pool, ctx, container, "POSTGRES")
	pgStr = fmt.Sprintf("postgres://usr:pw@localhost:%s/%s?sslmode=disable", hostPort, dbName)
	err = pool.Retry(func() error {
		log.Println("try pg connection...")
		var err error
		db, err := sql.Open("postgres", pgStr)
		if err != nil {
			return err
		}
		defer db.Close()
		return db.Ping()
	})
	return hostPort, container.Container.NetworkSettings.IPAddress, pgStr, err
}
