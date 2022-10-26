// Copyright 2022 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus-community/elasticsearch_exporter/pkg/clusterinfo"
	"github.com/prometheus/client_golang/prometheus"
)

type KnowNodes map[string]ClusterNodeStatsNodeResponse
type CurrentNodes map[string]ClusterNodeStatsNodeResponse

var (
	defaultClusterNodeLabels          = []string{"cluster", "name"}
	defaultKnowClusterNodeLabelValues = func(lastClusterinfo *clusterinfo.Response, node ClusterNodeStatsNodeResponse) []string {
		s := []string{}
		if lastClusterinfo != nil {
			s = append(s, lastClusterinfo.ClusterName)
		} else {
			s = append(s, "unknown_cluster")
		}
		s = append(s, node.Name)
		return s
	}
	knowNodes KnowNodes
)

type knowClusterNodeMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(knowNodes KnowNodes, currentNodes CurrentNodes, knowNodeName string, knowNode ClusterNodeStatsNodeResponse) float64
	Labels func(lastClusterinfo *clusterinfo.Response, node ClusterNodeStatsNodeResponse) []string
}

type clusterNodeMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(node ClusterNodeStatsNodeResponse) float64
	Labels func(lastClusterinfo *clusterinfo.Response, node ClusterNodeStatsNodeResponse) []string
}

// Cluster Nodes information struct
type ClusterNodes struct {
	logger          log.Logger
	client          *http.Client
	url             *url.URL
	clusterInfoCh   chan *clusterinfo.Response
	lastClusterInfo *clusterinfo.Response

	up                              prometheus.Gauge
	totalScrapes, jsonParseFailures prometheus.Counter

	clusterNodeMetrics     []*clusterNodeMetric
	knowClusterNodeMetrics []*knowClusterNodeMetric
}

func NewClusterNodes(logger log.Logger, client *http.Client, url *url.URL) *ClusterNodes {
	knowNodes = KnowNodes{}

	clusterNodes := &ClusterNodes{
		logger:        logger,
		client:        client,
		url:           url,
		clusterInfoCh: make(chan *clusterinfo.Response),
		lastClusterInfo: &clusterinfo.Response{
			ClusterName: "unknown_cluster",
		},

		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(namespace, "cluster_nodes", "up"),
			Help: "Was the last scrape of the Elasticsearch nodes endpoint successful.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "cluster_nodes", "total_scrapes"),
			Help: "Current total Elasticsearch node scrapes.",
		}),
		jsonParseFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "cluster_nodes", "json_parse_failures"),
			Help: "Number of errors while parsing JSON.",
		}),

		clusterNodeMetrics: []*clusterNodeMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "cluster_nodes", "elected_master"),
					"Elected master",
					defaultClusterNodeLabels, nil,
				),
				Value: func(node ClusterNodeStatsNodeResponse) float64 {
					if node.Master == "*" {
						return 1
					}
					return 0
				},
				Labels: defaultKnowClusterNodeLabelValues,
			},
		},
		knowClusterNodeMetrics: []*knowClusterNodeMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "cluster_nodes", "connected"),
					"Connected on cluster",
					defaultClusterNodeLabels, nil,
				),
				Value: func(knowNodes KnowNodes, currentNodes CurrentNodes, knowNodeName string, knowNode ClusterNodeStatsNodeResponse) float64 {
					if _, ok := currentNodes[knowNodeName]; ok {
						return 1
					} else {
						return 0
					}
				},
				Labels: defaultKnowClusterNodeLabelValues,
			},
		},
	}

	go func() {
		_ = level.Debug(logger).Log("msg", "starting cluster info receive loop")
		for ci := range clusterNodes.clusterInfoCh {
			if ci != nil {
				_ = level.Debug(logger).Log("msg", "received cluster info update", "cluster", ci.ClusterName)
				clusterNodes.lastClusterInfo = ci
			}
		}
		_ = level.Debug(logger).Log("msg", "exiting cluster info receive loop")
	}()

	return clusterNodes
}

func (cn *ClusterNodes) String() string {
	return namespace + "cluster_nodes"
}

func (cn *ClusterNodes) ClusterLabelUpdates() *chan *clusterinfo.Response {
	return &cn.clusterInfoCh
}

func (cn *ClusterNodes) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range cn.clusterNodeMetrics {
		ch <- metric.Desc
	}
	for _, metric := range cn.knowClusterNodeMetrics {
		ch <- metric.Desc
	}
	ch <- cn.up.Desc()
	ch <- cn.totalScrapes.Desc()
	ch <- cn.jsonParseFailures.Desc()
}

func (cn *ClusterNodes) fetchAndDecodeClusterNodes() (clusterNodesResponse, error) {
	var cnsr clusterNodesResponse

	u := *cn.url
	u.Path = path.Join(u.Path, "_cat/nodes")
	u.RawQuery = "format=json&h=name,master"

	res, err := cn.client.Get(u.String())
	if err != nil {
		return cnsr, fmt.Errorf("failed to get cluster nodes from %s://%s:%s%s: %s",
			u.Scheme, u.Hostname(), u.Port(), u.Path, err)
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			_ = level.Warn(cn.logger).Log(
				"msg", "failed to close http.Client",
				"err", err,
			)
		}
	}()

	if res.StatusCode != http.StatusOK {
		fmt.Println(res)
		return cnsr, fmt.Errorf("HTTP Request failed with code %d", res.StatusCode)
	}

	bts, err := ioutil.ReadAll(res.Body)
	if err != nil {
		cn.jsonParseFailures.Inc()
		return cnsr, err
	}

	if err := json.Unmarshal(bts, &cnsr); err != nil {
		cn.jsonParseFailures.Inc()
		return cnsr, err
	}
	return cnsr, nil
}

func (cn *ClusterNodes) Collect(ch chan<- prometheus.Metric) {
	cn.totalScrapes.Inc()
	defer func() {
		ch <- cn.up
		ch <- cn.totalScrapes
		ch <- cn.jsonParseFailures
	}()

	clusterNodesResponse, err := cn.fetchAndDecodeClusterNodes()
	if err != nil {
		cn.up.Set(0)
		_ = level.Warn(cn.logger).Log(
			"msg", "failed to fetch and decode node stats",
			"err", err,
		)
		return
	}
	cn.up.Set(1)

	currentNodes := CurrentNodes{}

	for _, node := range clusterNodesResponse {
		currentNodes[node.Name] = node
		knowNodes[node.Name] = node

		for _, metric := range cn.clusterNodeMetrics {
			ch <- prometheus.MustNewConstMetric(
				metric.Desc,
				metric.Type,
				metric.Value(node),
				metric.Labels(cn.lastClusterInfo, node)...,
			)
		}
	}

	for knowNodeName, knowNode := range knowNodes {
		for _, metric := range cn.knowClusterNodeMetrics {
			ch <- prometheus.MustNewConstMetric(
				metric.Desc,
				metric.Type,
				metric.Value(knowNodes, currentNodes, knowNodeName, knowNode),
				metric.Labels(cn.lastClusterInfo, knowNode)...,
			)
		}
	}
}
