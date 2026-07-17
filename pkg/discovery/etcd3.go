/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package discovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	etcd3 "go.etcd.io/etcd/client/v3"

	"seata.apache.org/seata-go/v2/pkg/util/log"
	"seata.apache.org/seata-go/v2/pkg/util/net"
)

const (
	clusterNameSplitChar = "-"
	addressSplitChar     = ":"
	etcdClusterPrefix    = "registry-seata"
)

type EtcdRegistryService struct {
	client        *etcd3.Client
	cfg           etcd3.Config
	vgroupMapping map[string]string
	store         *AddressStore

	stopCh    chan struct{}
	closeOnce sync.Once
}

func newEtcdRegistryService(config *ServiceConfig, etcd3Config *Etcd3Config) (RegistryService, error) {
	if config == nil {
		return nil, fmt.Errorf("service config is nil")
	}
	if etcd3Config == nil {
		return nil, fmt.Errorf("etcd config is nil")
	}

	cfg := etcd3.Config{
		Endpoints: []string{etcd3Config.ServerAddr},
	}
	cli, err := etcd3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd3 client: %w", err)
	}

	vgroupMapping := config.VgroupMapping

	etcdRegistryService := &EtcdRegistryService{
		client:        cli,
		cfg:           cfg,
		vgroupMapping: vgroupMapping,
		store:         NewAddressStore(),
		stopCh:        make(chan struct{}),
	}
	go etcdRegistryService.watch(etcdClusterPrefix)

	return etcdRegistryService, nil
}

func (s *EtcdRegistryService) watch(key string) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := s.client.Get(ctx, key, etcd3.WithPrefix())
	if err != nil {
		log.Infof("cant get server instances from etcd")
	}

	if resp != nil {
		for _, kv := range resp.Kvs {
			k := kv.Key
			v := kv.Value
			clusterName, err := getClusterName(k)
			if err != nil {
				log.Errorf("etcd key has an incorrect format: %v", err)
				return
			}
			serverInstance, err := getServerInstance(v)
			if err != nil {
				log.Errorf("etcd value has an incorrect format: %v", err)
				return
			}
			s.store.upsert(clusterName, serverInstance)
		}

	}
	// watch the changes of endpoints
	watchCh := s.client.Watch(ctx, key, etcd3.WithPrefix())

	for {
		select {
		case watchResp, ok := <-watchCh:
			if !ok {
				log.Warnf("Watch channel closed")
				return
			}
			for _, event := range watchResp.Events {
				switch event.Type {
				case etcd3.EventTypePut:
					log.Infof("Key %s updated. New value: %s\n", event.Kv.Key, event.Kv.Value)

					k := event.Kv.Key
					v := event.Kv.Value
					clusterName, err := getClusterName(k)
					if err != nil {
						log.Errorf("etcd key err: %v", err)
						return
					}
					serverInstance, err := getServerInstance(v)
					if err != nil {
						log.Errorf("etcd value err: %v", err)
						return
					}

					s.store.upsert(clusterName, serverInstance)

				case etcd3.EventTypeDelete:
					log.Infof("Key %s deleted.\n", event.Kv.Key)

					cluster, ip, port, err := getClusterAndAddress(event.Kv.Key)
					if err != nil {
						log.Errorf("etcd key err: %v", err)
						return
					}

					if !s.store.remove(cluster, ip, port) {
						log.Warnf("etcd instance not found. cluster: %s addr: %s:%d", cluster, ip, port)
					}
				}
			}
		case <-s.stopCh:
			log.Warn("stop etcd watch")
			return
		}
	}
}

func getClusterName(key []byte) (string, error) {
	stringKey := string(key)
	keySplit := strings.Split(stringKey, clusterNameSplitChar)
	if len(keySplit) != 4 {
		return "", fmt.Errorf("etcd key has an incorrect format. key: %s", stringKey)
	}

	cluster := keySplit[2]
	return cluster, nil
}

func getServerInstance(value []byte) (*ServiceInstance, error) {
	stringValue := string(value)
	ip, port, err := net.SplitIPPortStr(stringValue)
	if err != nil {
		return nil, fmt.Errorf("etcd port has an incorrect format. err: %w", err)
	}
	serverInstance := &ServiceInstance{
		Addr: ip,
		Port: port,
	}

	return serverInstance, nil
}

func getClusterAndAddress(key []byte) (string, string, int, error) {
	stringKey := string(key)
	keySplit := strings.Split(stringKey, clusterNameSplitChar)
	if len(keySplit) != 4 {
		return "", "", 0, fmt.Errorf("etcd key has an incorrect format. key: %s", stringKey)
	}
	cluster := keySplit[2]
	ip, port, err := net.SplitIPPortStr(keySplit[3])
	if err != nil {
		return "", "", 0, fmt.Errorf("etcd port has an incorrect format. err: %w", err)
	}

	return cluster, ip, port, nil
}

func (s *EtcdRegistryService) Lookup(key string) ([]*ServiceInstance, error) {
	cluster := s.vgroupMapping[key]
	if cluster == "" {
		return nil, fmt.Errorf("cluster doesnt exit")
	}

	return s.store.Snapshot(cluster), nil
}

func (s *EtcdRegistryService) Close() {
	s.closeOnce.Do(func() {
		if s.stopCh != nil {
			close(s.stopCh)
		}
		if s.client != nil {
			if err := s.client.Close(); err != nil && !errors.Is(err, context.Canceled) {
				log.Warnf("close etcd client failed: %v", err)
			}
		}
	})
}
