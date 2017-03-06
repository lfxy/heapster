package custom

import (
	"time"
	"fmt"
	//"net/url"
	"strings"
	"strconv"
	"crypto/tls"
	"net/http"

	. "k8s.io/heapster/metrics/core"
	"github.com/golang/glog"
	"k8s.io/heapster/metrics/sources/kubelet"
	"k8s.io/heapster/common/flags"
	//"runtime/debug"
)

type NodeInfo struct {
	kubelet.Host
	NodeName       string
	HostName       string
	HostID         string
	KubeletVersion string
}

type customMetricsSource struct {
	node          NodeInfo
	customClient  *CustomClient
}

type customProvider struct {
	customClient *CustomClient
}

//var customlist = []kubelet.Host{}
//var sources = []customMetricsSource{}
var sources = []MetricsSource{}

func NewCustomMetricsSource(node NodeInfo, client *CustomClient) MetricsSource {
	return &customMetricsSource{
		node:          node,
		customClient: client,
	}
}

func (this *customMetricsSource) Name() string {
	return this.String()
}

func (this *customMetricsSource) String() string {
	return fmt.Sprintf("custom:%s:%d", this.node.IP, this.node.Port)
}
func (this *customProvider) GetMetricsSources() []MetricsSource {
	/*for _, custom := range customlist {
		info := NodeInfo{
			Host: kubelet.Host{
				IP: custom.IP,
				Port: custom.Port,
			},
			NodeName: "nginx",
			HostName: "",
			HostID:   "",
			KubeletVersion: "",
		}

		sources = append(sources, NewCustomMetricsSource(info, this.kubeletClient, nil))
	}*/
	return sources
}

func (this *customMetricsSource) ScrapeMetrics(start, end time.Time) *DataBatch {
	result := &DataBatch{
		Timestamp: time.Now(),
		MetricSets: map[string]*MetricSet{},
	}
	custommetrics, err := func() (*map[string]map[string][]interface{}, error) {
		//startTime := time.Now()
		//var custommetrics map[string]map[string][]interface{}
		//defer summaryRequestLatency.WithLabelValues(this.node.HostName).Observe(float64(time.Since(startTime)))
		return this.customClient.GetCustomMetrics(this.node.Host)
	}()

	if err != nil {
		if kubelet.IsNotFoundError(err) {
			glog.Warningf("Summary not found, using fallback: %v", err)
			return result
		}
		glog.Errorf("error while getting metrics summary from Kubelet %s(%s:%d): %v", this.node.NodeName, this.node.IP, this.node.Port, err)
		return result
	}
	//glog.V(2).Infof("czq custom, ScrapeCustomMetrics:\n%s", custommetrics)
	result.MetricSets = this.decodeCustomData(custommetrics)

	return result
}

//func (this *summaryMetricsSource) decodeCustomData(custommetrics *stats.UserDefinedMetricItems) map[string]CustomMetricSet {
func (this *customMetricsSource) decodeCustomData(custommetrics *map[string]map[string][]interface{}) map[string]*MetricSet {
	result := map[string]*MetricSet{}

	for _, v := range *custommetrics {
		for _, v1 := range v {
			for _, v2 := range v1 {
				mvs := &MetricSet{
					MetricValues: map[string]MetricValue{},
				}
				if v3, ok := v2.(map[string]interface{}); ok {
					servername, s_exists := v3["server"];
					requestCounter, r_exists := v3["requestCounter"];
					if  s_exists && r_exists {
						mv := MetricValue{}
						mv.MetricType = MetricGauge
						mv.ValueType = ValueFloat
						if counter, ok := requestCounter.(float64); ok {
							mv.CustomValue = counter
						}
						mvs.MetricValues["qps"] = mv
						if name, ok := servername.(string); ok {
							if strings.Contains(name, ":") {
								names := strings.Split(name, ":")
								result[names[0]] = mvs
							} else {
								result[name] = mvs
							}
						}
					}
				}
			}
		}
	}
	return result
}

func NewCustomProvider(uris flags.Uris) (MetricsSourceProvider, error) {
	// create clients
	path := ""
	rq := ""
	if len(uris) > 0 {
		path = uris[0].Val.Path
		rq = uris[0].Val.RawQuery
	}
	customClient := createCustomClient(path, rq)
	for _, uri := range uris {
		glog.V(2).Infof("czq NewCustomProvider uri:%s, host:%s, path:%s, rawquery:%s", uri.String(), uri.Val.Host, uri.Val.Path, uri.Val.RawQuery)

		host := kubelet.Host{}
		if strings.Contains(uri.Val.Host, ":") {
			suburl := strings.Split(uri.Val.Host, ":")
			host.IP = suburl[0]
			iport, error := strconv.Atoi(suburl[1])
			if error != nil{
				glog.Errorf("NewCustomProvider port convert error! url:%s", uri.String())
				continue
			}
			host.Port = iport
		} else {
			host.IP = uri.Val.Host
			host.Port = 80
		}

		info := NodeInfo{
			Host: kubelet.Host{
				IP: host.IP,
				Port: host.Port,
			},
			NodeName: "nginx",
			HostName: "",
			HostID:   "",
			KubeletVersion: "",
		}

		sources = append(sources, NewCustomMetricsSource(info, customClient))
	}
	return &customProvider{
		customClient: customClient,
	}, nil
}

func createCustomClient(path, rawquery string) *CustomClient {
   tlsConfig := &tls.Config{
        InsecureSkipVerify: true,
    }

    transport := &http.Transport{
        TLSClientConfig: tlsConfig,
    }

	c := &http.Client{
		Transport: transport,
		//Timeout:   kubeletConfig.HTTPTimeout,
	}
	return &CustomClient {
		client:		c,
		path:		path,
		rawquery:	rawquery,
	}
}
