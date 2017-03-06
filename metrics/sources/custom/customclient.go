package custom

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"github.com/golang/glog"
	"k8s.io/heapster/metrics/sources/kubelet"
)

type CustomClient interface {
	GetCustomMetrics(host kubelet.Host) (*map[string]map[string][]interface{}, error)
}

type NginxCustomClient struct {
	client		*http.Client
	path		string
	rawquery    string
}

type HaproxyCustomClient struct {
	client		*http.Client
	path		string
	//rawquery    string
}

type ErrNotFound struct {
	endpoint string
}

func (err *ErrNotFound) Error() string {
	return fmt.Sprintf("%q not found", err.endpoint)
}

//func (self *CustomClient) postRequestAndGetValue(client *http.Client, req *http.Request, value interface{}) error {
func postRequestAndGetValue(client *http.Client, req *http.Request, value interface{}) error {
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	glog.V(2).Infof("czq kubelet_client.go, postRequestAndGetValue body:%s", string(body))
	if err != nil {
		return fmt.Errorf("failed to read response body - %v", err)
	}
	if response.StatusCode == http.StatusNotFound {
		return &ErrNotFound{req.URL.String()}
	} else if response.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed - %q, response: %q", response.Status, string(body))
	}
	err = json.Unmarshal(body, value)
	if err != nil {
		return fmt.Errorf("failed to parse output. Response: %q. Error: %v", string(body), err)
	}
	return nil
}

func (self *NginxCustomClient) GetCustomMetrics(host kubelet.Host) (*map[string]map[string][]interface{}, error) {
	url := url.URL{
		Scheme:		"http",
		Host:		fmt.Sprintf("%s:%d", host.IP, host.Port),
		Path:		self.path,
		RawQuery:	self.rawquery,
	}
	/*if self.config != nil && self.config.EnableHttps {
		url.Scheme = "https"
	}*/

	glog.V(2).Infof("czq kubelet_client.go, GetCustomMetrics url:%s", url.String())
	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	var custommetrics map[string]map[string][]interface{}
	client := self.client
	if client == nil {
		client = http.DefaultClient
	}
	err = postRequestAndGetValue(client, req, &custommetrics)
	return &custommetrics, err
}

func (self *HaproxyCustomClient) GetCustomMetrics(host kubelet.Host) (*map[string]map[string][]interface{}, error) {
	url := url.URL{
		Scheme:		"http",
		Host:		fmt.Sprintf("%s:%d", host.IP, host.Port),
		Path:		self.path,
		//RawQuery:	self.rawquery,
	}
	/*if self.config != nil && self.config.EnableHttps {
		url.Scheme = "https"
	}*/

	glog.V(2).Infof("czq kubelet_client.go, GetCustomMetrics url:%s", url.String())
	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	var custommetrics map[string]map[string][]interface{}
	client := self.client
	if client == nil {
		client = http.DefaultClient
	}
	err = postRequestAndGetValue(client, req, &custommetrics)
	return &custommetrics, err
}
