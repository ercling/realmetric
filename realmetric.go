package main

import (
	"bytes"
	"compress/zlib"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/yosuke-furukawa/json5/encoding/json5"
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
var env *Env
var Conf *Config

type DbConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Charset  string
	Timeout  int
}

type GinConfig struct {
	Mode string
	Host string
	Port int
	User string
	Password string
}

type Config struct {
	Db                         DbConfig
	MetricNameValidationRegexp string
	SliceNameValidationRegexp  string
	Gin                        GinConfig
}

type Env struct {
	db *sql.DB
}

type metricsCache struct {
	mu    sync.Mutex
	cache map[string]int
}

type slicesCache struct {
	mu    sync.Mutex
	cache map[string]map[string]int
	db    *sql.DB
}

type TrackData struct {
	Metric string
	Slices map[string]string
	Time   int64
	Value  int
	Minute int
}

type DailyMetric struct {
	metricId  int
	value     int
	minute    int
	timestamp int64
}

type DailyMetricsStorage struct {
	mu    sync.Mutex
	store map[string]map[string]DailyMetric
	env   *Env
}

func (config *Config) Init() error {
	//read config
	jsonFile, err := os.Open("config.json5")
	defer jsonFile.Close()
	if err != nil {
		return err
	}
	dec := json5.NewDecoder(jsonFile)
	err = dec.Decode(config)

	return err
}

func (store *DailyMetricsStorage) Inc(id int, event TrackData) bool {
	store.mu.Lock()
	var key string
	eventTime := time2.Unix(event.Time, 0)
	dateKey := eventTime.Format("2006_01_02")
	key = strconv.Itoa(id) + "_" + strconv.Itoa(event.Minute)
	fmt.Println(key)
	_, ok := store.store[dateKey]
	if ok {
		val, ok := store.store[dateKey][key]
		if ok {
			val.value = val.value + event.Value
			store.store[dateKey][key] = val
		} else {
			store.store[dateKey][key] = DailyMetric{metricId: id, minute: event.Minute, value: event.Value, timestamp: event.Time}
		}

	} else {
		store.store = make(map[string]map[string]DailyMetric)
		_, ok := store.store[dateKey]
		if !ok {
			store.store[dateKey] = make(map[string]DailyMetric)
		}
		store.store[dateKey][key] = DailyMetric{metricId: id, minute: event.Minute, value: event.Value, timestamp: event.Time}
	}
	store.mu.Unlock()
	return true
}

func (store *DailyMetricsStorage) FlushToDb() int {
	store.mu.Lock()
	vals := []interface{}{}
	if store.store == nil {
		log.Println("DailyMetricsStore is empty")
		store.mu.Unlock()
		return 0
	}

	tableCreated := false
	for dateKey, values := range store.store {
		tableName := "daily_metrics_" + dateKey
		//create table
		if !tableCreated {
			uniqueName := tableName + "_metric_id_minute_unique"
			indexName := tableName + "_metric_id_index"
			sqlStr := "CREATE TABLE IF NOT EXISTS " + tableName +
				" (`id` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`metric_id` smallint(5) unsigned NOT NULL," +
				"`value` int(11) NOT NULL," +
				"`minute` smallint(5) unsigned NOT NULL," +
				"PRIMARY KEY (`id`)," +
				"UNIQUE KEY " + uniqueName + " (`metric_id`,`minute`)," +
				"KEY " + indexName + " (`metric_id`)" +
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"
			//TODO: add error log here
			stmt, err := env.db.Prepare(sqlStr)
			if err != nil {
				log.Fatal(err)
			}
			stmt.Exec()
			tableCreated = true
		}

		//insert rows
		sqlStr := "INSERT INTO " + tableName + " (metric_id, value, minute) VALUES "

		for _, dailyMetric := range values {
			sqlStr += "(?, ?, ?),"
			vals = append(vals, dailyMetric.metricId, dailyMetric.value, dailyMetric.minute)
		}
		//trim the last ,
		sqlStr = sqlStr[0 : len(sqlStr)-1]
		sqlStr += " ON DUPLICATE KEY UPDATE `value` = `value` + VALUES(`value`)"

		//TODO: add error logs
		//prepare the statement
		stmt, _ := env.db.Prepare(sqlStr)

		//format all vals at once
		stmt.Exec(vals...)
	}
	store.store = nil

	store.mu.Unlock()
	return len(vals)
}

//func (store *DailyMetricsStorage) SetTableFromMinute(minute int) string{
//
//}

func (mc *metricsCache) GetMetricIdByName(metricName string) (int, error) {
	val, ok := mc.cache[metricName]
	if !ok {
		crc32name := crc32.ChecksumIEEE([]byte(metricName))
		row := env.db.QueryRow("SELECT id from metrics where name_crc_32=?", crc32name)
		// TODO: implement metric creation
		var id int
		err := row.Scan(&id)
		if err != nil {
			log.Fatal(err)
			return 0, err
		}
		val = id
		mc.cache[metricName] = val
	}
	return val, nil
}

func (td *TrackData) FillMinute() error {
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
	var tracks []TrackData
	if err := json.Unmarshal(jsonBytes, &tracks); err != nil {
		log.Fatal(err)
	}

	createdEvents := aggregate(tracks)

	c.JSON(http.StatusAccepted, gin.H{
		"createdEvents": createdEvents,
		"_timing":       time2.Since(startTime).Nanoseconds(),
	})

	//io.Copy(os.Stdout, zReader)
	//fmt.Fprintf(w, "Post from website! r.PostFrom = %v\n", r.PostForm)
	//fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
}

func main() {
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
	env = &Env{db: db}
	DailyMetricsStore.env = env

	rows, err := db.Query("SELECT id, name FROM metrics")
	if err != nil {
		log.Fatal(err)
		return
	}

	r, err := regexp.Compile(Conf.MetricNameValidationRegexp)
	//TODO: add slices regexp here
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
	//slices
	rows, err = db.Query("SELECT id, category, name FROM slices")
	if err != nil {
		log.Fatal(err)
		return
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

	ticker := time2.NewTicker(5 * time2.Second)
	go func() {
		for t := range ticker.C {
			fmt.Println(t.Clock())
			DailyMetricsStore.FlushToDb()
		}
	}()

	//http.HandleFunc("/track", handler)
	//log.Fatal(http.ListenAndServe(":8080", nil))
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

func aggregate(tracks []TrackData) int {
	r, err := regexp.Compile(Conf.MetricNameValidationRegexp)
	if err != nil {
		log.Panic(err)
		os.Exit(1)
	}
	counter := 0
	for _, element := range tracks {
		if r.MatchString(element.Metric) {
			log.Println("Skip invalid metric: " + element.Metric)
			continue
		}
		element.FillMinute()
		metricId, err := MetricsCache.GetMetricIdByName(element.Metric)
		if err != nil {
			log.Println("Cannot get metric id: " + element.Metric)
			continue
		}
		if DailyMetricsStore.Inc(metricId, element) {
			counter++
		}
	}
	//TODO: add slices here
	return counter
}
