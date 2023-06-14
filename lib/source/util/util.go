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

package util

import (
	"github.com/segmentio/kafka-go"
	"github.com/wvanbergen/kazoo-go"
	"io/ioutil"
	"log"
	"strings"
)

func GetBroker(zk string) (brokers []string, err error) {
	return getBroker(zk)
}

func getBroker(zkUrl string) (brokers []string, err error) {
	zookeeper := kazoo.NewConfig()
	zookeeper.Logger = log.New(ioutil.Discard, "", 0)
	zk, chroot := kazoo.ParseConnectionString(zkUrl)
	zookeeper.Chroot = chroot
	if kz, err := kazoo.NewKazoo(zk, zookeeper); err != nil {

		return brokers, err
	} else {
		brokers, err = kz.BrokerList()

		return brokers, err
	}
}

func InitTopic(zkUrl string, configMap map[string][]kafka.ConfigEntry, topics ...string) (err error) {
	return InitTopicWithConfig(zkUrl, configMap, 1, 1, topics...)
}

func InitTopicWithConfig(kafkaUrl string, configMap map[string][]kafka.ConfigEntry, numPartitions int, replicationFactor int, topics ...string) (err error) {
	initConn, err := kafka.Dial("tcp", kafkaUrl)
	if err != nil {
		return err
	}
	defer initConn.Close()
	for _, topic := range topics {
		err = initConn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
			ConfigEntries:     GetTopicConfig(configMap, topic),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func GetTopicConfig(configMap map[string][]kafka.ConfigEntry, topic string) []kafka.ConfigEntry {
	if configMap == nil {
		return nil
	}
	result, exists := configMap[topic]
	if exists {
		return result
	}
	for key, conf := range configMap {
		if strings.HasPrefix(topic, key) {
			return conf
		}
	}
	return nil
}
