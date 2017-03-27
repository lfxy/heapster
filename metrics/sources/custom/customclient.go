package custom

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"encoding/csv"
	"strings"
	"strconv"
	"time"


	"github.com/golang/glog"
	"k8s.io/heapster/metrics/sources/kubelet"
)

/*
type CustomClient interface {
	GetCustomMetrics(host kubelet.Host) (*map[string]map[string][]interface{}, error)
}*/

type NginxCustomClient struct {
	client		*http.Client
	path		string
	rawquery    string
}

type HaproxyCustomClient struct {
	client		*http.Client
	path		string
}

type ErrNotFound struct {
	endpoint string
}

func (err *ErrNotFound) Error() string {
	return fmt.Sprintf("%q not found", err.endpoint)
}

//func (self *CustomClient) postRequestAndGetValue(client *http.Client, req *http.Request, value interface{}) error {
func (self *NginxCustomClient) postRequestAndGetValue(client *http.Client, req *http.Request, value interface{}) error {
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
	err = self.postRequestAndGetValue(client, req, &custommetrics)
	return &custommetrics, err
}

func (self *HaproxyCustomClient) postRequestAndGetValue(client *http.Client, req *http.Request, value *map[string]float64, testvalues *map[int]float64, port int) error {
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	//glog.V(2).Infof("czq HaproxyCustomClient postRequestAndGetValue body:%s", string(body))
	if err != nil {
		return fmt.Errorf("failed to read response body - %v", err)
	}
	if response.StatusCode == http.StatusNotFound {
		return &ErrNotFound{req.URL.String()}
	} else if response.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed - %q, response: %q", response.Status, string(body))
	}


	csvreader := csv.NewReader(strings.NewReader(string(body)))
	datas, err := csvreader.ReadAll()
	sz := len(datas)
	if err != nil || sz == 0{
		return fmt.Errorf("failed to parse output. Response: %q. Error: %v", string(body), err)
	}
	svindex, reqindex := -1, -1
	for i, name := range datas[0] {
		if name == "stot" {
			reqindex = i
		}
		if name == "svname" {
			svindex = i
		}
	}
	for i := 1; i < sz; i++ {
		if strings.HasPrefix(datas[i][svindex], "server") {
			key := datas[i][svindex][6:]
			newvalue, err := strconv.ParseFloat(datas[i][reqindex], 64)
			if err != nil {
				glog.Errorf("error while parsing haproxy metrics :%s, %v", datas[i], err)
				continue
			}
			if metrics, exists := (*value)[key]; exists {
				(*value)[key] = metrics + newvalue
			} else {
				(*value)[key] = newvalue
			}

			//czq test
			if strings.Contains(datas[i][0], "php-apache") {
				if _, exs := (*testvalues)[port]; exs {
					glog.Errorf("error test test!!!!!")
				} else {
					(*testvalues)[port] = newvalue
				}
			}
			//czq test
		}
	}
	return nil
}

func (self *HaproxyCustomClient) GetCustomMetrics(ip string, port int, metricsvalues *map[string]float64, testvalues *map[int]float64) (error) {
	url := url.URL {
		Scheme:		"http",
		Host:		fmt.Sprintf("%s:%d", ip, port),
		Path:		self.path,
	}

	glog.V(2).Infof("czq kubelet_client.go, haproxy GetCustomMetrics url:%s", url.String())
	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return err
	}
	client := self.client
	if client == nil {
		client = http.DefaultClient
	}
	err = self.postRequestAndGetValue(client, req, metricsvalues, testvalues, port)

	return err
}

func (self *HaproxyCustomClient) GetReloadTime(ip string, port int) (map[string]time.Time, error) {
	result := make(map[string]time.Time)
	url := url.URL {
		Scheme:		"http",
		Host:		fmt.Sprintf("%s:%d", ip, port),
		Path:		self.path,
	}

	glog.V(2).Infof("czq kubelet_client.go, GetReloadTime url:%s", url.String())
	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return result, err
	}
	client := self.client
	if client == nil {
		client = http.DefaultClient
	}

	err = self.postReloadTimeReq(client, req, &result)

	return result, err
}

func (self *HaproxyCustomClient) postReloadTimeReq(client *http.Client, req *http.Request, result *map[string]time.Time) (error) {
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	glog.V(2).Infof("czq kubelet_client.go, postReloadTimeReq body:%s", string(body))
	if err != nil {
		return fmt.Errorf("failed to read response body - %v", err)
	}
	if response.StatusCode == http.StatusNotFound {
		return &ErrNotFound{req.URL.String()}
	} else if response.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed - %q, response: %q", response.Status, string(body))
	}

	//to do
	timearr := strings.Split(string(body), "\n")
	if len(timearr) != 2 {
		return fmt.Errorf("response body does not have right time:%q", string(body))
	}

	lasttime, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", timearr[0])
	if err != nil {
		glog.Errorf("czq postReloadTimeReq error lasttime:%s", timearr[0])
		return err
	}
	currenttime, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", timearr[1])
	if err != nil {
		glog.Errorf("czq postReloadTimeReq error currenttime:%s", timearr[1])
		return err
	}
	(*result)["LastReloadTime"] = lasttime.Truncate(time.Second)
	(*result)["CurrentTime"] = currenttime.Truncate(time.Second)

	return nil
}
