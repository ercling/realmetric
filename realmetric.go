package main

import (
	"bytes"
	"compress/zlib"
	"database/sql"
	"encoding/json"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"hash/crc32"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/http/pprof"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	time2 "time"
)

var MCache metricsCache
var SlicesCache slicesCache
var DailyMetricsStore DailyMetricsStorage
var DailyMetricsTotals DailyMetricsTotalsStorage
var DailySlicesStore DailySlicesStorage
var DailySlicesTotals DailySlicesTotalsStorage
var Db *sql.DB
var Conf *Config

type InsertData struct {
	TableName string
	Fields    []string
	Values    []interface{}
}

func (portions *InsertData) AppendValues(args ...interface{}) {
	portions.Values = append(portions.Values, args...)
}

func (portions *InsertData) InsertIncrementBatch() {
	portionCount := 40000
	portionCount = portionCount - (portionCount % len(portions.Fields))

	currPortionNumber := 0
	countOfPortions := int(math.Ceil(float64(len(portions.Values)) / float64(portionCount)))

	for currPortionNumber < countOfPortions {
		startSlice := portionCount * currPortionNumber
		endSlice := portionCount * (currPortionNumber + 1)

		currCap := len(portions.Values)
		if currCap < endSlice {
			endSlice = currCap
		}
		Slice := portions.Values[startSlice:endSlice]
		groupRepeatCount := len(Slice) / len(portions.Fields)
		log.Println("endSlice: " + strconv.Itoa(endSlice) + " startSlice: " + strconv.Itoa(startSlice) + " portionLenFields: " + strconv.Itoa(len(portions.Fields)) + " SlicesLen:" + strconv.Itoa(len(Slice)))
		fieldsStr := strings.Join(portions.Fields, ",")
		SqlStr := "INSERT INTO " + portions.TableName + " (" + fieldsStr + ") VALUES "

		questionGroup := strings.Repeat("?,", len(portions.Fields))
		questionGroup = questionGroup[0 : len(questionGroup)-1]
		SqlStr += strings.Repeat("("+questionGroup+"),", groupRepeatCount)
		SqlStr = SqlStr[0 : len(SqlStr)-1]
		SqlStr += " ON DUPLICATE KEY UPDATE `value` = `value` + VALUES(`value`)"

		stmt, err := Db.Prepare(SqlStr)
		if err != nil {
			log.Print("Table: " + portions.TableName + " ")
			log.Panic(err)
		}
		_, err = stmt.Exec(Slice...)
		stmt.Close()
		if err != nil {
			log.Print("Table: " + portions.TableName + " ")
			log.Println(err)
			bytesJ, _ := json.Marshal(Slice)
			log.Println(string(bytesJ))
		} else {
			log.Println("Inserted")
		}

		currPortionNumber++
	}
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

type Event struct {
	Metric string
	Slices map[string]string `json:"slices,omitempty"`
	Time   int64
	Value  int
	Minute int
}

func (sc *slicesCache) GetSliceIdByCategoryAndName(category string, name string) (int, error) {
	sc.mu.Lock()
	sliceId, ok := sc.cache[category][name]
	if ok {
		sc.mu.Unlock()
		return sliceId, nil
	}

	crc32category := crc32.ChecksumIEEE([]byte(category))
	crc32name := crc32.ChecksumIEEE([]byte(name))
	var id int
	err := Db.QueryRow("SELECT id FROM slices WHERE category_crc_32=? AND name_crc_32=?", crc32category, crc32name).Scan(&id)
	if err != nil {
		//create id
		stmt, es := Db.Prepare("INSERT IGNORE INTO slices (category, category_crc_32, name, name_crc_32) VALUES (?, ?, ?, ?)")
		if es != nil {
			log.Panic(es)
		}
		result, err := stmt.Exec(category, crc32category, name, crc32name)
		if err != nil {
			log.Panic(err)
		}
		insertId, _ := result.LastInsertId()
		id = int(insertId)
	}
	sliceId = id
	if _, ok = sc.cache[category]; !ok {
		sc.cache[category] = make(map[string]int)
	}
	sc.cache[category][name] = sliceId
	sc.mu.Unlock()
	return sliceId, nil
}

func (mc *metricsCache) GetMetricIdByName(metricName string) (int, error) {
	mc.mu.Lock()
	metricId, ok := mc.cache[metricName]
	if ok {
		mc.mu.Unlock()
		return metricId, nil
	}
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

	metricId = id
	mc.cache[metricName] = metricId
	mc.mu.Unlock()
	return metricId, nil
}

func (td *Event) FillMinute() error {
	time := time2.Unix(td.Time, 0)

	time, err := LocalTime(time)
	if err != nil {
		log.Panic(err)
	}
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

	go aggregateEvents(tracks)

	c.JSON(http.StatusAccepted, gin.H{
		"createdEvents": 42,
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

	createTables()
	warmupMetricsCache()
	warmupSlicesCache()
}

func createTables(){
	//monthly_metrics
	sqlStr := "CREATE TABLE IF NOT EXISTS `monthly_metrics` ("+
	"`id` int(10) unsigned NOT NULL AUTO_INCREMENT,"+
		"`metric_id` smallint(5) unsigned NOT NULL,"+
		"`value` int(11) unsigned NOT NULL,"+
		"`date` date NOT NULL,"+
		"PRIMARY KEY (`id`),"+
		"UNIQUE KEY `monthly_metrics_metric_id_date_unique` (`metric_id`,`date`),"+
		"KEY `monthly_metrics_metric_id_index` (`metric_id`)"+
	") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"
	stmt, err := Db.Prepare(sqlStr)
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	//monthly_slices
	sqlStr = "CREATE TABLE IF NOT EXISTS `monthly_slices` ("+
	"`id` int(10) unsigned NOT NULL AUTO_INCREMENT,"+
		"`metric_id` smallint(5) unsigned NOT NULL,"+
		"`slice_id` smallint(5) unsigned NOT NULL,"+
		"`value` int(11) unsigned NOT NULL,"+
		"`date` date NOT NULL,"+
		"PRIMARY KEY (`id`),"+
		"UNIQUE KEY `monthly_slices_metric_id_slice_id_date_unique` (`metric_id`,`slice_id`,`date`),"+
		"KEY `monthly_slices_metric_id_slice_id_index` (`metric_id`,`slice_id`),"+
		"KEY `metric_date` (`metric_id`,`date`)"+
	") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"
	stmt, err = Db.Prepare(sqlStr)
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	//metrics
	sqlStr = "CREATE TABLE IF NOT EXISTS `metrics` ("+
	"`id` int(10) unsigned NOT NULL AUTO_INCREMENT,"+
		"`name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,"+
		"`name_crc_32` int(10) unsigned NOT NULL,"+
		"PRIMARY KEY (`id`),"+
		"UNIQUE KEY `metrics_name_unique` (`name`),"+
		"KEY `metrics_name_crc_32_index` (`name_crc_32`)"+
	") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"
	stmt, err = Db.Prepare(sqlStr)
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	//slices
	sqlStr = "CREATE TABLE IF NOT EXISTS `slices` ("+
	"`id` int(10) unsigned NOT NULL AUTO_INCREMENT,"+
		"`category` varchar(255) COLLATE utf8_unicode_ci NOT NULL,"+
		"`category_crc_32` int(10) unsigned NOT NULL,"+
		"`name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,"+
		"`name_crc_32` int(10) unsigned NOT NULL,"+
		"PRIMARY KEY (`id`),"+
		"KEY `slices_category_crc_32_name_crc_32_index` (`category_crc_32`,`name_crc_32`)"+
	") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"
	stmt, err = Db.Prepare(sqlStr)
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}
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
	MCache.mu.Lock()
	MCache.cache = cacheMetrics
	MCache.mu.Unlock()
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
		err = rows.Scan(&id, &category, &name)
		if err != nil {
			log.Println("Skip unresolved slice row")
			continue
		}
		if r.MatchString(name) {
			log.Println("Skip slice by regexp: " + name)
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
	//DailySlicesTotals = DailySlicesTotalsStorage{}
	//DailyMetricsStore = DailyMetricsStorage{}
	//DailyMetricsTotals = DailyMetricsTotalsStorage{}
	//DailySlicesStore = DailySlicesStorage{}
	//DailySlicesTotals = DailySlicesTotalsStorage{}

	go func() {
		debugR := mux.NewRouter()
		debugR.HandleFunc("/debug/pprof/", pprof.Index)
		debugR.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		debugR.HandleFunc("/debug/pprof/profile", pprof.Profile)
		debugR.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		debugR.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		debugR.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		debugR.Handle("/debug/pprof/block", pprof.Handler("block"))
		debugR.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		err := http.ListenAndServe("localhost:6060", debugR)
		if err != nil {
			log.Println(err)
		}
	}()

	//start flush daily_metrics ticker
	ticker := time2.NewTicker(time2.Duration(Conf.FlushToDbInterval) * time2.Second)
	go func() {
		for range ticker.C {
			DailyMetricsStore.FlushToDb()
			DailySlicesStore.FlushToDb()
		}
	}()
	//start flush daily_metric_totals ticker
	ticker2 := time2.NewTicker(time2.Duration(Conf.FlushTotalsInterval) * time2.Second)
	go func() {
		for range ticker2.C {
			DailyMetricsTotals.FlushToDb()
			DailySlicesTotals.FlushToDb()
		}
	}()

	//setup gin
	gin.SetMode(Conf.Gin.Mode)
	server := gin.Default()
	//add cors
	config := cors.DefaultConfig()
	config.AllowCredentials = true
	config.AllowAllOrigins = true
	server.Use(cors.New(config))
	authorized := server.Group("/", gin.BasicAuth(gin.Accounts{Conf.Gin.User: Conf.Gin.Password}))

	server.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	authorized.POST("/track", trackHandler)
	if Conf.Gin.TlsEnabled {
		server.RunTLS(Conf.Gin.Host+":"+strconv.Itoa(Conf.Gin.Port), Conf.Gin.TlsCertFilePath, Conf.Gin.TlsKeyFilePath)
	} else {
		server.Run(Conf.Gin.Host + ":" + strconv.Itoa(Conf.Gin.Port))
	}

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
		metricId, err := MCache.GetMetricIdByName(event.Metric)
		if err != nil {
			log.Println("Cannot get metric id: " + event.Metric)
			continue
		}
		if DailyMetricsStore.Inc(metricId, event) && DailyMetricsTotals.Inc(metricId, event) {
			counter++
		}
		//slices
		if event.Slices == nil {
			continue
		}

		for category, name := range event.Slices {
			sliceId, err := SlicesCache.GetSliceIdByCategoryAndName(category, name)
			if err != nil {
				log.Println("Cannot get metric id: " + event.Metric)
				continue
			}
			DailySlicesStore.Inc(metricId, sliceId, event)
			DailySlicesTotals.Inc(metricId, sliceId, event)
		}

	}
	return counter
}

func LocalTime(t time2.Time) (time2.Time, error) {
	//loc, err := time2.LoadLocation(Conf.TimeZone)
	//if err == nil {
	//	t = t.In(loc)
	//}
	return t, nil
}
