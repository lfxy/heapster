package custom

import (
	"time"
	"fmt"
	"net/url"
	"strings"
	"strconv"
	"crypto/tls"
	"net/http"

	. "k8s.io/heapster/metrics/core"
	"github.com/golang/glog"
	"k8s.io/heapster/metrics/sources/kubelet"
	//"k8s.io/heapster/common/flags"
	kube_config "k8s.io/heapster/common/kubernetes"
	kube_client "k8s.io/kubernetes/pkg/client/unversioned"
	kube_api "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/client/cache"
	//"runtime/debug"
)

const (
	nginxPath		=	"/vts_status/control"
	nginxRawQuery	=	"cmd=status&group=upstream@group&zone=*"
	haproxyPath		=	"status;csv;norefresh"
)

type LbInfo struct {
	kubelet.Host
	LbName       string
	//HostName       string
	//HostID         string
	//KubeletVersion string
}

type customProvider struct {
	configMapNamespaceLister    *cache.ConfigMapNamespaceLister
	reflector					*cache.Reflector
	nginxCustomClient			*NginxCustomClient
	haproxyCustomClient			*HaproxyCustomClient
	configMapNames				string
}

type NginxQpsMetricsSource struct {
	lb					LbInfo
	nginxCustomClient  *NginxCustomClient
}

func NewNginxMetricsSource(node LbInfo, client *NginxCustomClient) MetricsSource {
	return &NginxQpsMetricsSource{
		lb:          node,
		nginxCustomClient: client,
	}
}

func (this *NginxQpsMetricsSource) Name() string {
	return this.String()
}

func (this *NginxQpsMetricsSource) String() string {
	return fmt.Sprintf("custom:%s:%d", this.lb.IP, this.lb.Port)
}

func (this *NginxQpsMetricsSource) ScrapeMetrics(start, end time.Time) *DataBatch {
	result := &DataBatch{
		Timestamp: time.Now(),
		MetricSets: map[string]*MetricSet{},
	}
	custommetrics, err := func() (*map[string]map[string][]interface{}, error) {
		//startTime := time.Now()
		//var custommetrics map[string]map[string][]interface{}
		//defer summaryRequestLatency.WithLabelValues(this.node.HostName).Observe(float64(time.Since(startTime)))
		return this.nginxCustomClient.GetCustomMetrics(this.lb.Host)
	}()

	if err != nil {
		if kubelet.IsNotFoundError(err) {
			glog.Warningf("Summary not found, using fallback: %v", err)
			return result
		}
		glog.Errorf("error while getting metrics summary from Kubelet %s(%s:%d): %v", this.lb.LbName, this.lb.IP, this.lb.Port, err)
		return result
	}
	//glog.V(2).Infof("czq custom, ScrapeCustomMetrics:\n%s", custommetrics)
	result.MetricSets = this.decodeCustomData(custommetrics)

	return result
}

func (this *NginxQpsMetricsSource) decodeCustomData(custommetrics *map[string]map[string][]interface{}) map[string]*MetricSet {
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

type HaproxyQpsMetricsSource struct {
	lb					LbInfo
	haproxyCustomClient  *HaproxyCustomClient
}

func NewHaproxyMetricsSource(node LbInfo, client *HaproxyCustomClient) MetricsSource {
	return &HaproxyQpsMetricsSource{
		lb:          node,
		haproxyCustomClient: client,
	}
}

func (this *HaproxyQpsMetricsSource) Name() string {
	return this.String()
}

func (this *HaproxyQpsMetricsSource) String() string {
	return fmt.Sprintf("custom:%s:%d", this.lb.IP, this.lb.Port)
}

func (this *HaproxyQpsMetricsSource) ScrapeMetrics(start, end time.Time) *DataBatch {
	result := &DataBatch{
		Timestamp: time.Now(),
		MetricSets: map[string]*MetricSet{},
	}
	custommetrics, err := func() (*map[string]map[string][]interface{}, error) {
		return this.haproxyCustomClient.GetCustomMetrics(this.lb.Host)
	}()

	if err != nil {
		if kubelet.IsNotFoundError(err) {
			glog.Warningf("Summary not found, using fallback: %v", err)
			return result
		}
		glog.Errorf("error while getting metrics summary from Kubelet %s(%s:%d): %v", this.lb.LbName, this.lb.IP, this.lb.Port, err)
		return result
	}
	//glog.V(2).Infof("czq custom, ScrapeCustomMetrics:\n%s", custommetrics)
	result.MetricSets = this.decodeCustomData(custommetrics)

	return result
}

func (this *HaproxyQpsMetricsSource) decodeCustomData(custommetrics *map[string]map[string][]interface{}) map[string]*MetricSet {
	result := map[string]*MetricSet{}


	return result
}

func (this *customProvider) GetMetricsSources() []MetricsSource {
	sources := []MetricsSource{}
	cmnames := strings.Split(this.configMapNames, ",")
	for _, configmapname := range cmnames {
		configmap, err := this.configMapNamespaceLister.Get(configmapname)
		if err != nil {
			glog.Errorf("error while listing configmap: %v", err)
			continue
		}
		if configmap == nil {
			glog.Error("No configmap received from APIserver.")
			continue
		}

		if strips, exists := configmap.Data["cluster.ips"]; exists {
			ips := strings.Split(strips, ",")
			for _, ip := range ips {
				glog.V(2).Infof("czq customProvider GetMetricsSources:%s", ip)
				host := kubelet.Host{}
				if strings.Contains(ip, ":") {
					suburl := strings.Split(ip, ":")
					host.IP = suburl[0]
					iport, error := strconv.Atoi(suburl[1])
					if error != nil{
						glog.Errorf("NewCustomProvider port convert error! url:%s", ip)
						continue
					}
					host.Port = iport
				} else {
					host.IP = ip
					host.Port = 80
				}

				info := LbInfo{
					Host: kubelet.Host{
						IP: host.IP,
						Port: host.Port,
					},
					LbName: configmapname,
				}

				if strings.Contains(configmapname, "nginx") {
					sources = append(sources, NewNginxMetricsSource(info, this.nginxCustomClient))
				} else if strings.Contains(configmapname, "haproxy") {
					sources = append(sources, NewHaproxyMetricsSource(info, this.haproxyCustomClient))
				}
			}
		}
	}
	return sources
}

func NewCustomProvider(uri *url.URL, lbNames string) (MetricsSourceProvider, error) {
	kubeConfig, err := kube_config.GetKubeClientConfig(uri)
	if err != nil {
		glog.Fatalf("Failed to get client config: %v", err)
	}
	kubeClient := kube_client.NewOrDie(kubeConfig)

	lw := cache.NewListWatchFromClient(kubeClient, "configmaps", kube_api.NamespaceAll, fields.Everything())
	store := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	configMapLister := &cache.ConfigMapLister{Indexer: store}
	reflector := cache.NewReflector(lw, &kube_api.ConfigMap{}, store, time.Hour)
	reflector.Run()

	configmapNamespaceLister := configMapLister.ConfigMaps("kube-system")

	nginxCustomClient := createNginxClient(nginxPath, nginxRawQuery)
	haproxyCustomClient := createHaproxyClient(haproxyPath)
	return &customProvider {
		configMapNamespaceLister:	&configmapNamespaceLister,
		reflector:					reflector,
		nginxCustomClient:			nginxCustomClient,
		haproxyCustomClient:		haproxyCustomClient,
		configMapNames:				lbNames,
	}, nil
}

func createNginxClient(path, rawquery string) *NginxCustomClient {
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
	return &NginxCustomClient {
		client:		c,
		path:		path,
		rawquery:	rawquery,
	}
}

func createHaproxyClient(path string) *HaproxyCustomClient {
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
	return &HaproxyCustomClient {
		client:		c,
		path:		path,
	}
}
