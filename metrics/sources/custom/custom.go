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
	//"k8s.io/kubernetes/pkg/client/restclient"

	//"k8s.io/kubernetes/pkg/api/v1"
	//"k8s.io/apimachinery/pkg/runtime"
	//"k8s.io/kubernetes/pkg/runtime"
	//api_fields "k8s.io/apimachinery/pkg/fields"
	//metav1 "k8s.io/kubernetes/vendor/k8s.io/apimachinery/pkg/apis/meta/v1"
	//api_fields "k8s.io/apimachinery/pkg/fields"
	//metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//"runtime/debug"
	//"k8s.io/kubernetes/pkg/controller"
	//"k8s.io/client-go/1.4/pkg/api"
	//watch "k8s.io/client-go/1.4/pkg/watch"
	//client_cache "k8s.io/client-go/tools/cache"
)

const (
	nginxPath				=	"/vts_status/control"
	nginxRawQuery			=	"cmd=status&group=upstream@group&zone=*"
	nginxControllerPort		=	33611
	haproxyPath				=	"status;csv;norefresh"
	haproxyReloadPath		=	"statstimes"
	haproxyControllerIp		=	"10.209.224.13"
	haproxyControllerPort	=	33611
	customNamespace			=	"wanda-ingress"
)

var haproxyPorts = [32]int{8091,8092,8093,8094,8095,8096,8097,8098,8099,8100,8101,8102,8103,8104,8105,8106,8107,8108,8109,8110,8111,8112,8113,8114,8115,8116,8117,8118,8119,8120,8121,8122}

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
	haproxyReloadClient			*HaproxyCustomClient
	configMapNames				string
}

func (this *customProvider) GetMetricsSources(name string) []MetricsSource {
	sources := []MetricsSource{}
	cmnames := strings.Split(this.configMapNames, ",")
	for _, configmapname := range cmnames {
		if strings.Contains(configmapname, name) {
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
						host.Port = nginxControllerPort
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
    }
	return sources
}

func (this *customProvider) GetReloadTime() (ReloadTime, error) {
	rt := ReloadTime {
		LbName:		"haproxy",
	}

	host := kubelet.Host{
		IP:		"",
		Port:	-1,
	}
	cmnames := strings.Split(this.configMapNames, ",")
	for _, configmapname := range cmnames {
		if strings.Contains(configmapname, "haproxy") {
			configmap, err := this.configMapNamespaceLister.Get(configmapname)
			if err != nil {
				glog.Errorf("error while listing configmap: %v", err)
				continue
			}
			if configmap == nil {
				glog.Errorf("No configmap received from APIserver.")
				continue
			}

			if strips, exists := configmap.Data["cluster.ips"]; exists {
				ips := strings.Split(strips, ",")
				for _, ip := range ips {
					glog.V(2).Infof("czq customProvider GetReloadTime:%s", ip)
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
						host.Port = nginxControllerPort
					}

					break
				}
			}
			break
		}
    }

	if host.IP != "" {
		mrt, err := this.haproxyReloadClient.GetReloadTime(host.IP, host.Port)
		if err != nil {
			return rt, err
		}
		if mrt["CurrentTime"].Before(mrt["LastReloadTime"]) {
			return rt, fmt.Errorf("haproxy reload time error")
		}
		rt.LastReloadTime = mrt["LastReloadTime"]
		rt.CurrentTime = mrt["CurrentTime"]
		return rt, nil
	} else {
		return rt, fmt.Errorf("get error haproxy controller ip")
	}
}

func NewCustomProvider(uri *url.URL, lbNames string) (MetricsSourceProvider, error) {
	kubeConfig, err := kube_config.GetKubeClientConfig(uri)
	if err != nil {
		glog.Errorf("Failed to get client config: %v", err)
		return nil, err
	}
	kubeClient := kube_client.NewOrDie(kubeConfig)

/* if strings.Contains(lbNames, "haproxy") {
		resyncPeriod := 60*time.Second
		hc, _ := NewHaproxyController(kubeClient, resyncPeriod, customNamespace, lbNames)
		go hc.Run()
	}*/

	lw := cache.NewListWatchFromClient(kubeClient, "configmaps", kube_api.NamespaceAll, fields.Everything())
	//store := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	store := cache.NewStore(cache.MetaNamespaceKeyFunc)
	configMapLister := &cache.ConfigMapLister{Store: store}
	reflector := cache.NewReflector(lw, &kube_api.ConfigMap{}, store, time.Hour)
	reflector.Run()

	configmapNamespaceLister := configMapLister.ConfigMaps(customNamespace)

	nginxCustomClient := createNginxClient(nginxPath, nginxRawQuery)
	haproxyCustomClient := createHaproxyClient(haproxyPath)
	haproxyReloadClient := createHaproxyClient(haproxyReloadPath)
	return &customProvider {
		configMapNamespaceLister:	&configmapNamespaceLister,
		reflector:					reflector,
		nginxCustomClient:			nginxCustomClient,
		haproxyCustomClient:		haproxyCustomClient,
		haproxyReloadClient:		haproxyReloadClient,
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
						if counter, ok := requestCounter.(float64); ok {
							mv.CustomValue = counter
						} else {
							glog.Errorf("error nginx metrics :%s, %v", servername, requestCounter)
							continue
						}
						mv.MetricType = MetricGauge
						mv.ValueType = ValueFloat
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

	tmpret := make(map[string]float64)
	testret := make(map[int]float64)

	for _, port := range haproxyPorts {
		err := this.haproxyCustomClient.GetCustomMetrics(this.lb.Host.IP, port, &tmpret, &testret)
		if err != nil {
			glog.Errorf("error while getting metrics summary from Kubelet %s(%s:%d): %v", this.lb.LbName, this.lb.IP, this.lb.Port, err)
			return result
		}
	}
	/*custommetrics, err := func() (*map[string]map[string][]interface{}, error) {
		return this.haproxyCustomClient.GetCustomMetrics(this.lb.Host)
	}()*/

	//glog.V(2).Infof("czq custom, ScrapeCustomMetrics:\n%s", tmpret)
	result.MetricSets = this.decodeCustomData(&tmpret)

	for k, v := range tmpret {
		glog.V(2).Infof("czq HaproxyQpsMetricsSource, ScrapeMetrics:%s:%f, ", k, v)
	}
	for k, v := range testret {
		glog.V(2).Infof("czq HaproxyQpsMetricsSource, ScrapeMetrics:%d:%f, ", k, v)
	}
	return result
}

func (this *HaproxyQpsMetricsSource) decodeCustomData(custommetrics *map[string]float64) map[string]*MetricSet {
	result := map[string]*MetricSet{}

	for k, v := range *custommetrics {
		mvs := &MetricSet{
			MetricValues: map[string]MetricValue{},
		}
		mv := MetricValue{}
		mv.CustomValue = v
		mv.MetricType = MetricGauge
		mv.ValueType = ValueFloat
		mvs.MetricValues["qps"] = mv
		result[k] = mvs
	}

	return result
}

