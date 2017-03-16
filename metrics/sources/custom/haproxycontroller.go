package custom


import(
	"strings"
	"time"
	"reflect"

	kube_client "k8s.io/kubernetes/pkg/client/unversioned"
	kube_api "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/watch"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/fields"
	"github.com/golang/glog"
)


type HaproxyMetricsController struct {
	Client						*kube_client.Client
	ResyncPeriod				time.Duration
	cfgmLister					cache.ConfigMapLister
	haproxyController			*cache.Controller
	cfgmQueue					*TaskQueue
	stopCh						chan struct{}
}

func (hmc *HaproxyMetricsController) syncCfgm(key string) {
	glog.V(2).Infof("czq1 HaproxyMetricsController syncCfgm:%s", key)
	obj, cfgmExists, err := hmc.cfgmLister.Store.GetByKey(key)
	if err != nil {
		hmc.cfgmQueue.Requeue(key, err)
		return
	}

	if cfgmExists {
		cfgm := obj.(*kube_api.ConfigMap)
		if clusterIPs, exists := cfgm.Data["cluster.ips"]; exists {
			glog.V(2).Infof("czq1 HaproxyMetricsController syncCfgm ips:%s", clusterIPs)
		}
	}
}

func NewHaproxyController(kubeClient *kube_client.Client, resyncPeriod time.Duration, namespace string, lbNames string) (*HaproxyMetricsController, error) {
	glog.V(2).Infof("czq1 NewHaproxyController")
	hmc := HaproxyMetricsController{
		Client:			kubeClient,
		ResyncPeriod:	resyncPeriod,
		stopCh:          make(chan struct{}),
	}
	hmc.cfgmQueue = NewTaskQueue(hmc.syncCfgm)
	/*kubeconfig2, err := restclient.InClusterConfig()
	if err != nil {
		//kubeconfig2, err = clientConfig.ClientConfig()
		if err != nil {
			glog.Fatalf("error configuring the client: %v", err)
		}
	}
	cmClient2, err := kube_client.New(kubeconfig2)
	if err != nil {
		glog.Fatalf("failed to create client: %v", err)
	}*/


	/*kubeConfig, err := kube_config.GetKubeClientConfig(uri)
	if err != nil {
		glog.Fatalf("Failed to get client config: %v", err)
	}
	kubeClient := kube_client.NewOrDie(kubeConfig)*/

	cmnames := strings.Split(lbNames, ",")
	for _, configmapname := range cmnames {
		if strings.Contains(configmapname, "haproxy") {
			glog.V(2).Infof("czq1 NewHaproxyController :%s", configmapname)

			configMapSelector := fields.SelectorFromSet(map[string]string{kube_api.ObjectNameField: configmapname})
			hmc.cfgmLister.Store, hmc.haproxyController = cache.NewInformer(
				&cache.ListWatch{
					ListFunc: func(lo kube_api.ListOptions) (runtime.Object, error) {
						lo.FieldSelector = configMapSelector
						return kubeClient.ConfigMaps("kube-system").List(lo)
					},
					WatchFunc: func(lo kube_api.ListOptions) (watch.Interface, error) {
						lo.FieldSelector = configMapSelector
						return kubeClient.ConfigMaps("kube-system").Watch(lo)
					},
				},
				&kube_api.ConfigMap{},
				resyncPeriod,
				cache.ResourceEventHandlerFuncs{
					AddFunc: func(obj interface{}) {
						glog.V(2).Infof("czq1 NewHaproxyController add")
						hmc.cfgmQueue.Enqueue(obj)
					},
					UpdateFunc: func(old, cur interface{}) {
						glog.V(2).Infof("czq1 NewHaproxyController update")
						if !reflect.DeepEqual(old, cur) {
							hmc.cfgmQueue.Enqueue(cur)
						}
					},
					DeleteFunc: func(obj interface{}) {
						glog.V(2).Infof("czq1 NewHaproxyController delete")
						hmc.cfgmQueue.Enqueue(obj)
					},
				},
			)
			//hmc.cfgmLister =  cache.ConfigMapLister{Store: store}

			break;
		}
	}
	return &hmc, nil
}

func (hmc *HaproxyMetricsController) Run() {
	glog.V(2).Infof("czq1 HaproxyMetricsController Run")
	go hmc.haproxyController.Run(hmc.stopCh)
	go hmc.cfgmQueue.Run(time.Second, hmc.stopCh)
	<-hmc.stopCh
	hmc.cfgmQueue.Shutdown()
	glog.V(2).Infof("czq1 HaproxyMetricsController Run 2")
}
