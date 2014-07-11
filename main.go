package main

import (
	"bufio"
	_ "database/sql"
	"encoding/json"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

var wg sync.WaitGroup
var db *sqlx.DB
var urls_c chan Exchange
var proxy Proxy

type Company struct {
	Id  int
	Url string `db:"company_site"`
}

func (comp Company) updateCompanyStatus(status string) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	tx.MustExec(
		"UPDATE company set status = $1, last_site_refresh = $2 where id = $3",
		status,
		time.Now(),
		comp.Id,
	)
	tx.Commit()

	return nil
}

type Exchange struct {
	Comp    Company
	Retries int
}

func (exc Exchange) addToChannel() {
	wg.Add(1)
	urls_c <- exc
}

type Configuration struct {
	DbType              string
	DbConnection        string
	GoRoutinesMaxNumber int
}

func getConf(confFile string) (*Configuration, error) {
	file, err := os.Open(confFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	conf := Configuration{}
	err = json.NewDecoder(file).Decode(&conf)
	if err != nil {
		return nil, err
	}
	return &conf, nil
}

type Proxy struct {
	proxies [](*url.URL)
}

func (proxy *Proxy) loadProxies(fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		proxy_url, _ := url.Parse("http://" + scanner.Text())
		proxy.proxies = append(proxy.proxies, proxy_url)
	}
	return nil
}

func (proxy Proxy) getProxy(request *http.Request) (*url.URL, error) {
	return proxy.proxies[rand.Intn(len(proxy.proxies))], nil
}

func TimeoutDialer(cTimeout time.Duration) func(net, addr string) (c net.Conn, err error) {
	return func(netw, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(netw, addr, cTimeout)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
}

func doRequest(
	req *http.Request,
	connTimeout time.Duration,
	rwTimeount time.Duration,
	proxyFunc func(*http.Request) (*url.URL, error),
) (*http.Response, error, bool) {
	transport := &http.Transport{
		Dial:  TimeoutDialer(connTimeout),
		Proxy: proxyFunc,
	}
	client := &http.Client{Transport: transport}

	timeout := false
	timer := time.AfterFunc(rwTimeount, func() {
		transport.CancelRequest(req)
		timeout = true
	})
	resp, err := client.Do(req)
	if timer != nil {
		timer.Stop()
	}

	return resp, err, timeout
}

func checkSite(exc Exchange) {
	defer wg.Done()

	req, _ := http.NewRequest("HEAD", exc.Comp.Url, nil)
	resp, err, timeout := doRequest(req, 5*time.Second, 10*time.Second, proxy.getProxy)

	if timeout {
		log.Printf("%#v - TIMEOUT\n", exc.Comp.Url)
		exc.Comp.updateCompanyStatus("not_active")
		return
	}

	if err != nil {
		if exc.Retries < 3 {
			exc.Retries += 1
			exc.addToChannel()
		}
		err := err.(*url.Error)
		log.Printf("<<< REQUEST ERROR: URL - %#v, ERR - %#v\n", err.URL, err.Err)
		return
	}

	defer resp.Body.Close()
	log.Printf("%#v - %#v\n", exc.Comp.Url, resp.StatusCode)

	var status string
	if resp.StatusCode == http.StatusOK {
		status = "active"
	} else {
		status = "not_active"
	}
	err = exc.Comp.updateCompanyStatus(status)
	if err != nil {
		if exc.Retries < 3 {
			exc.Retries += 1
			exc.addToChannel()
		}
		log.Printf("<<< DATABASE ERROR - %#v", err)
	}
}

func main() {
	t := time.Now()

	proxy = Proxy{}
	err := proxy.loadProxies("proxies_list_by_ip.txt")
	if err != nil {
		log.Fatalln(err)
	}

	conf, err := getConf("conf.json")
	if err != nil {
		log.Fatalln(err)
	}

	urls_c = make(chan Exchange, conf.GoRoutinesMaxNumber)

	db = sqlx.MustConnect(conf.DbType, conf.DbConnection)
	comps := []Company{}
	err = db.Select(&comps, "SELECT id, company_site FROM company ORDER BY ID LIMIT 300")
	if err != nil {
		log.Fatalln(err)
	}

	for i := 0; i < conf.GoRoutinesMaxNumber; i++ {
		go func() {
			for {
				exc := <-urls_c
				checkSite(exc)
			}
		}()
	}

	for _, comp := range comps {
		exc := Exchange{Comp: comp, Retries: 0}
		exc.addToChannel()
	}

	wg.Wait()
	log.Printf("time taken - %#v", time.Now().Sub(t).Seconds())
}
