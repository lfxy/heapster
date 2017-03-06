// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sources

import (
	"math/rand"
	"time"
	"fmt"

	. "k8s.io/heapster/metrics/core"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	//"runtime/debug"
	"k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/labels"
)

const (
	DefaultMetricsScrapeTimeout = 20 * time.Second
	MaxDelayMs                  = 4 * 1000
	DelayPerSourceMs            = 8
)

var (
	// Last time Heapster performed a scrape since unix epoch in seconds.
	lastScrapeTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "heapster",
			Subsystem: "scraper",
			Name:      "last_time_seconds",
			Help:      "Last time Heapster performed a scrape since unix epoch in seconds.",
		},
		[]string{"source"},
	)

	// Time spent exporting scraping sources in microseconds..
	scraperDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "heapster",
			Subsystem: "scraper",
			Name:      "duration_microseconds",
			Help:      "Time spent scraping sources in microseconds.",
		},
		[]string{"source"},
	)
)

func init() {
	prometheus.MustRegister(lastScrapeTimestamp)
	prometheus.MustRegister(scraperDuration)
}

func NewSourceManager(metricsSourceProvider MetricsSourceProvider, customProvider MetricsSourceProvider, metricsScrapeTimeout time.Duration, podLister *cache.StoreToPodLister) (MetricsSource, error) {
	return &sourceManager{
		metricsSourceProvider: metricsSourceProvider,
		customProvider:		   customProvider,
		metricsScrapeTimeout:  metricsScrapeTimeout,
		podLister:			   podLister,
		oldCustomResult:	   DataBatch{Timestamp: time.Now().Add(-time.Hour * 1), MetricSets: map[string]*MetricSet{},},
		//rcLister:			   rcLister,
		//ingressLister:			   ingressLister,
	}, nil
}

type sourceManager struct {
	metricsSourceProvider MetricsSourceProvider
    customProvider MetricsSourceProvider
	metricsScrapeTimeout  time.Duration
	podLister  *cache.StoreToPodLister
	oldCustomResult DataBatch
	//rcLister *cache.StoreToReplicationControllerLister
	//ingressLister *cache.StoreToIngressLister
}

func (this *sourceManager) Name() string {
	return "source_manager"
}

func (this *sourceManager) ScrapeMetrics(start, end time.Time) *DataBatch {
	//glog.V(2).Infof("czq sources/manager.go  ScrapeMetrics--------------s")
	//glog.V(2).Infof("czq sources/manager.go ScrapeMetrics:\n%s", string(debug.Stack()))
	sources := this.metricsSourceProvider.GetMetricsSources()
	//glog.V(2).Infof("czq sources/manager.go  ScrapeMetrics--------------e:%d", len(sources))
	glog.V(1).Infof("Scraping metrics start: %s, end: %s", start, end)

	responseChannel := make(chan *DataBatch)
	startTime := time.Now()
	timeoutTime := startTime.Add(this.metricsScrapeTimeout)

	delayMs := DelayPerSourceMs * len(sources)
	if delayMs > MaxDelayMs {
		delayMs = MaxDelayMs
	}

	for _, source := range sources {

		go func(source MetricsSource, channel chan *DataBatch, start, end, timeoutTime time.Time, delayInMs int) {

			// Prevents network congestion.
			time.Sleep(time.Duration(rand.Intn(delayMs)) * time.Millisecond)

			glog.V(2).Infof("Querying source: %s", source)
			metrics := scrape(source, start, end)
			now := time.Now()
			if !now.Before(timeoutTime) {
				glog.Warningf("Failed to get %s response in time", source)
				return
			}
			timeForResponse := timeoutTime.Sub(now)

			select {
			case channel <- metrics:
				// passed the response correctly.
				return
			case <-time.After(timeForResponse):
				glog.Warningf("Failed to send the response back %s", source)
				return
			}
		}(source, responseChannel, start, end, timeoutTime, delayMs)
	}
	response := DataBatch{
		Timestamp:  end,
		MetricSets: map[string]*MetricSet{},
	}

	latencies := make([]int, 11)

responseloop:
	for i := range sources {
		now := time.Now()
		if !now.Before(timeoutTime) {
			glog.Warningf("Failed to get all responses in time (got %d/%d)", i, len(sources))
			break
		}

		select {
		case dataBatch := <-responseChannel:
			if dataBatch != nil {
				for key, value := range dataBatch.MetricSets {
					response.MetricSets[key] = value
				}
			}
			latency := now.Sub(startTime)
			bucket := int(latency.Seconds())
			if bucket >= len(latencies) {
				bucket = len(latencies) - 1
			}
			latencies[bucket]++

		case <-time.After(timeoutTime.Sub(now)):
			glog.Warningf("Failed to get all responses in time (got %d/%d)", i, len(sources))
			break responseloop
		}
	}


	err := this.scrapeCustomMetrics(start, end, delayMs, timeoutTime, &response)
	if err != nil {
		glog.Warningf("Failed to get custom metrics %s", err)
	}

	for key, value := range response.MetricSets {
		glog.V(2).Infof("czq sources/manager.go ScrapeMetrics key:%s-------s", key)
		for metrickey, metricvalue := range value.MetricValues {
			glog.V(2).Infof("czq mkey:%s, mvalue:%f", metrickey, metricvalue.FloatValue)
		}
		glog.V(2).Infof("czq sources/manager.go ScrapeMetrics key:%s-------e", key)
	}
	glog.V(1).Infof("ScrapeMetrics: time: %s size: %d", time.Since(startTime), len(response.MetricSets))
	for i, value := range latencies {
		glog.V(1).Infof("   scrape  bucket %d: %d", i, value)
	}
	return &response
}

func (this *sourceManager) scrapeCustomMetrics(start, end time.Time, delayMs int, timeoutTime time.Time, response *DataBatch) error {
	var b_success = true
	customsources := this.customProvider.GetMetricsSources()
	if len(customsources) == 0 {
		return fmt.Errorf("No custom metrics sources and failed get custom metrics!")
	}
	customResponseChannel := make(chan *DataBatch)
	for _, customsource := range customsources {
		go func(source MetricsSource, channel chan *DataBatch, start, end, timeoutTime time.Time, delayInMs int) {

			// Prevents network congestion.
			time.Sleep(time.Duration(rand.Intn(delayMs)) * time.Millisecond)

			//metrics := scrape(source, start, end)
			customemetrics := source.ScrapeMetrics(start, end)
			now := time.Now()
			if !now.Before(timeoutTime) {
				b_success = false
				glog.Warningf("Failed to get %s response in time", source)
				return
			}
			timeForResponse := timeoutTime.Sub(now)

			select {
			case channel <- customemetrics:
				// passed the response correctly.
				return
			case <-time.After(timeForResponse):
				glog.Warningf("Failed to send the response back %s", source)
				b_success = false
				return
			}
		}(customsource, customResponseChannel, start, end, timeoutTime, delayMs)

	}

	customresponse := DataBatch{
		Timestamp:  end,
		MetricSets: map[string]*MetricSet{},
	}
customresponseloop:
	for i := range customsources {
		now := time.Now()
		if !now.Before(timeoutTime) {
			glog.Warningf("Failed to get all responses in time (got %d/%d)", i, len(customsources))
			b_success = false
			break
		}

		select {
		case customDataBatch := <-customResponseChannel:
			if customDataBatch != nil && len(customDataBatch.MetricSets) > 0 {
				for h_key, h_value := range customDataBatch.MetricSets {
					oldvalue, exists := customresponse.MetricSets[h_key]
					if exists {
						for l_key, l_value := range h_value.MetricValues {
							l_oldvalue, l_exits := oldvalue.MetricValues[l_key]
							if l_exits {
								mv := l_oldvalue
								mv.CustomValue += l_value.CustomValue
								oldvalue.MetricValues[l_key] = mv
							} else {
								mv := MetricValue{
									MetricType: l_value.MetricType,
									ValueType:  l_value.ValueType,
									CustomValue: l_value.CustomValue,
								}

								customresponse.MetricSets[h_key].MetricValues[l_key] = mv
							}
						}
					} else {
						ms := &MetricSet{
							MetricValues: map[string]MetricValue{},
						}
						for l_key, l_value := range h_value.MetricValues {
							mv := MetricValue{
								MetricType: l_value.MetricType,
								ValueType: l_value.ValueType,
								CustomValue: l_value.CustomValue,
							}
							ms.MetricValues[l_key] = mv
						}
						customresponse.MetricSets[h_key] = ms
					}

				}
			} else {
				b_success = false
				break customresponseloop
			}

		case <-time.After(timeoutTime.Sub(now)):
			glog.Warningf("Failed to get custom responses in time (got %d/%d)", i, len(customsources))
			b_success = false
			break customresponseloop
		}
	}

	if !b_success {
		return fmt.Errorf("Get custom metrics failed for there are unavailable curstom resources")
	}

	glog.V(2).Infof("czq sources/manager.go ScrapeCustomMetrics old:%s, old_add:%s, end:%s:", this.oldCustomResult.Timestamp, this.oldCustomResult.Timestamp.Add(end.Sub(start) + time.Second * 5), end)
	if this.oldCustomResult.Timestamp.Add(end.Sub(start) + time.Second * 5).After(end) {
		labelSelector, _ := labels.Parse("")
		pods, _ := this.podLister.List(labelSelector)
		var podIpToName map[string]string
		podIpToName = make(map[string]string)
		for _, pod := range pods {
			glog.V(2).Infof("czq podLister :%s, %s", pod.Name, pod.Status.PodIP)
			podIpToName[pod.Status.PodIP] = PodKey(pod.Namespace, pod.Name)
		}

		for h_key, h_value := range customresponse.MetricSets {
			if podname, pod_exist := podIpToName[h_key]; pod_exist {
				responstvalue, r_exists := response.MetricSets[podname]
				oldvalue, c_oldexists := this.oldCustomResult.MetricSets[h_key]
				if c_oldexists && r_exists {
					for l_key, l_value := range h_value.MetricValues {
						if l_oldvalue, l_exits := oldvalue.MetricValues[l_key]; l_exits {
							if l_value.CustomValue < l_oldvalue.CustomValue {
								continue
							}
							l_value.FloatValue = float32(l_value.CustomValue - l_oldvalue.CustomValue) / float32(60)
							responstvalue.MetricValues[CustomMetricPrefix + l_key] = l_value
						}
					}
				} else {
					glog.Warningf("this ip:%s does not exist in old values", h_key)
				}
			} else {
				glog.Warningf("this ip:%s does not exist in pod list", h_key)
			}
		}
	} else {
		this.oldCustomResult = customresponse
		return fmt.Errorf("The old custom metrics has no data or has time out")
	}

	for k1, v1 := range this.oldCustomResult.MetricSets {
		glog.V(2).Infof("czq sources/manager.go ScrapeMetrics old CustomResult key:%s, v: %s", k1, *v1)
	}
	for k1, v1 := range customresponse.MetricSets {
		glog.V(2).Infof("czq sources/manager.go ScrapeMetrics new CustomResult key:%s, v: %s", k1, *v1)
	}
	this.oldCustomResult = customresponse
	return nil
}

func scrape(s MetricsSource, start, end time.Time) *DataBatch {
	sourceName := s.Name()
	startTime := time.Now()
	defer lastScrapeTimestamp.
		WithLabelValues(sourceName).
		Set(float64(time.Now().Unix()))
	defer scraperDuration.
		WithLabelValues(sourceName).
		Observe(float64(time.Since(startTime)) / float64(time.Microsecond))

	return s.ScrapeMetrics(start, end)
}
