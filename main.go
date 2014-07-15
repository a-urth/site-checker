package main

import (
	"bufio"
	_ "database/sql"
	"encoding/json"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/parnurzeal/gorequest"
	"log"
	"math/rand"
	"net/http"
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
	proxies []string
}

func (proxy *Proxy) loadProxies(fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		proxy.proxies = append(proxy.proxies, "http://"+scanner.Text())
	}
	return nil
}

func (proxy *Proxy) getProxy() string {
	return proxy.proxies[rand.Intn(len(proxy.proxies))]
}

func checkSite(exc Exchange) {
	defer wg.Done()

	req := gorequest.New().Proxy(proxy.getProxy()).Timeout(10 * time.Second)
	resp, _, errs := req.Get(exc.Comp.Url).End()

	if len(errs) != 0 {
		if exc.Retries < 3 {
			exc.Retries += 1
			exc.addToChannel()
		}
		log.Printf("<<< REQUEST ERROR <<< | URL %#v", exc.Comp.Url)
		for _, err := range errs {
			log.Printf("Error - %#v", err)
		}
		return
	}

	log.Printf("%#v - %#v\n", exc.Comp.Url, resp.StatusCode)

	var status string
	if resp.StatusCode == http.StatusOK {
		status = "active"
	} else {
		status = "not_active"
	}
	err := exc.Comp.updateCompanyStatus(status)
	if err != nil {
		if exc.Retries < 3 {
			exc.Retries += 1
			exc.addToChannel()
		}
		log.Printf("<<< DATABASE ERROR <<< | ERR %#v | URL - %#v", err, exc.Comp.Url)
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
	err = db.Select(&comps, "SELECT id, company_site FROM company ORDER BY last_site_refresh LIMIT 10000")
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
