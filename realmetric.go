package main

import (
	"bytes"
	"compress/zlib"
	"database/sql"
	"encoding/json"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"hash/crc32"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	time2 "time"
)

var MetricsCache metricsCache
var SlicesCache slicesCache
var DailyMetricsStore DailyMetricsStorage
var Db *sql.DB
var Conf *Config

type metricsCache struct {
	mu    sync.Mutex
	cache map[string]int
}

type slicesCache struct {
	mu    sync.Mutex
	cache map[string]map[string]int
	db    *sql.DB
}

type Event struct {
	Metric string
	Slices map[string]string `json:"slices,omitempty"`
	Time   int64
	Value  int
	Minute int
}

func (mc *metricsCache) GetMetricIdByName(metricName string) (int, error) {
	val, ok := mc.cache[metricName]
	if !ok {
		crc32name := crc32.ChecksumIEEE([]byte(metricName))
		var id int
		err := Db.QueryRow("SELECT id from metrics where name_crc_32=?", crc32name).Scan(&id)

		if err != nil {
			//create id
			stmt, es := Db.Prepare("INSERT IGNORE INTO metrics (name, name_crc_32) VALUES (?, ?)")
			if es != nil {
				log.Panic(es)
			}
			result, err := stmt.Exec(metricName, crc32name)
			if err != nil {
				log.Panic(err)
			}
			insertId, _ := result.LastInsertId()
			id = int(insertId)
		}

		val = id
		mc.cache[metricName] = val
	}
	return val, nil
}

func (td *Event) FillMinute() error {
	time := time2.Unix(td.Time, 0)
	td.Minute = time.Hour()*60 + time.Minute()
	return nil
}

//func handler(w http.ResponseWriter, r *http.Request) {
func trackHandler(c *gin.Context) {
	startTime := time2.Now()
	body, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		log.Fatal(err)
		return
	}

	zReader, err := zlib.NewReader(bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
		return
	}
	defer zReader.Close()

	jsonBytes, err := ioutil.ReadAll(zReader)
	if err != nil {
		log.Fatal(err)
		return
	}
	var tracks []Event
	//var jsonData []map[string]interface{}

	if err := json.Unmarshal(jsonBytes, &tracks); err != nil {
		log.Println(err.Error() + "Json: " + string(jsonBytes))
		c.JSON(http.StatusBadRequest, gin.H{
			"createdEvents": 0,
			"_timing":       time2.Since(startTime).Nanoseconds(),
		})
		return
	}

	//for _, value := range jsonData {
	//	track := Event{}
	//	metric, ok := value["metric"]
	//	if !ok {
	//		log.Println("Skipped: No metric")
	//		continue
	//	}
	//	track.Metric, ok = metric.(string)
	//	if !ok {
	//		log.Println("Skipped: Cant cast metric to string")
	//		continue
	//	}
	//
	//	metricVal, ok := value["value"]
	//	if !ok {
	//		log.Println("Skipped: no metricVal")
	//		continue
	//	}
	//	mv := metricVal.(float64)
	//	if !ok {
	//		log.Println("Skipped: Cant cast value to float64")
	//		continue
	//	}
	//	track.Value = int(mv)
	//
	//	valTime := time2.Now().Unix()
	//	metricTime, ok := value["time"]
	//	if ok {
	//		valT, ok := metricTime.(float64)
	//		if ok {
	//			valTime = int64(valT)
	//		}
	//	}
	//	track.Time = valTime
	//
	//	slices, ok := value["slices"]
	//	if ok {
	//		slc := make(map[string]string)
	//		for category, sliceVal := range slices.(map[string]interface{}) {
	//			slc[category] = sliceVal.(string)
	//		}
	//		track.Slices = slc
	//	}
	//
	//	tracks = append(tracks, track)
	//
	//}

	createdEvents := aggregateEvents(tracks)

	c.JSON(http.StatusAccepted, gin.H{
		"createdEvents": createdEvents,
		"_timing":       time2.Since(startTime).Nanoseconds(),
	})

}

func init() {
	Conf = &Config{}
	err := Conf.Init()
	if err != nil {
		log.Fatal(err)
		return
	}

	dsn := Conf.Db.User + ":" + Conf.Db.Password + "@tcp(" + Conf.Db.Host + ":" + strconv.Itoa(Conf.Db.Port) + ")/" + Conf.Db.Database + "?charset=" + Conf.Db.Charset + "&timeout=" + strconv.Itoa(Conf.Db.Timeout) + "s&sql_mode=TRADITIONAL&autocommit=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
		return
	}
	Db = db

	warmupMetricsCache()
	warmupSlicesCache()
}

func warmupMetricsCache() {
	rows, err := Db.Query("SELECT id, name FROM metrics")
	if err != nil {
		log.Fatal(err)
		return
	}

	r, err := regexp.Compile(Conf.MetricNameValidationRegexp)
	if err != nil {
		log.Panic(err)
		os.Exit(1)
	}
	cacheMetrics := make(map[string]int)
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Println("Skip unresolved metric row")
			continue
		}
		if r.MatchString(name) {
			log.Println("Skip metric by regexp: " + name)
			continue
		}
		cacheMetrics[name] = id

	}
	MetricsCache.mu.Lock()
	MetricsCache.cache = cacheMetrics
	MetricsCache.mu.Unlock()
}

func warmupSlicesCache() {
	rows, err := Db.Query("SELECT id, category, name FROM slices")
	if err != nil {
		log.Fatal(err)
		return
	}
	r, err := regexp.Compile(Conf.SliceNameValidationRegexp)
	if err != nil {
		log.Panic(err)
		os.Exit(1)
	}
	cacheSlices := make(map[string]map[string]int)
	for rows.Next() {
		var id int
		var name string
		var category string
		err = rows.Scan(&id, &name, &category)
		if err != nil {
			log.Println("Skip unresolved metric row")
			continue
		}
		if r.MatchString(name) {
			log.Println("Skip metric by regexp: " + name)
			continue
		}
		if _, ok := cacheSlices[category]; !ok {
			cacheSlices[category] = make(map[string]int)
		}
		cacheSlices[category][name] = id
	}
	SlicesCache.mu.Lock()
	SlicesCache.cache = cacheSlices
	SlicesCache.mu.Unlock()

}

func main() {
	//start flush ticker
	ticker := time2.NewTicker(time2.Duration(Conf.FlushToDbInterval) * time2.Second)
	go func() {
		for range ticker.C {
			DailyMetricsStore.FlushToDb()
		}
	}()

	//setup gin
	gin.SetMode(Conf.Gin.Mode)
	server := gin.Default()
	authorized := server.Group("/", gin.BasicAuth(gin.Accounts{Conf.Gin.User: Conf.Gin.Password}))

	server.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	authorized.POST("/track", trackHandler)
	server.Run(Conf.Gin.Host + ":" + strconv.Itoa(Conf.Gin.Port))
}

func aggregateEvents(tracks []Event) int {
	r, err := regexp.Compile(Conf.MetricNameValidationRegexp)
	if err != nil {
		log.Panic(err)
	}

	counter := 0
	for _, event := range tracks {
		if r.MatchString(event.Metric) {
			log.Println("Skip invalid metric: " + event.Metric)
			continue
		}
		event.FillMinute()
		metricId, err := MetricsCache.GetMetricIdByName(event.Metric)
		if err != nil {
			log.Println("Cannot get metric id: " + event.Metric)
			continue
		}
		if DailyMetricsStore.Inc(metricId, event) {
			counter++
		}
	}
	//TODO: add slices here
	return counter
}
