package main

import (
	"log"
	"strconv"
	"sync"
	"time"
)

type DailyMetric struct {
	metricId  int
	value     int
	minute    int
	timestamp int64
}

type DailyMetricsStorage struct {
	mu    sync.Mutex
	store map[string]map[string]DailyMetric
}

func (store *DailyMetricsStorage) Inc(id int, event Event) bool {
	store.mu.Lock()
	var key string
	eventTime := time.Unix(event.Time, 0)
	dateKey := eventTime.Format("2006_01_02")
	key = strconv.Itoa(id) + "_" + strconv.Itoa(event.Minute)
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
		//log.Println("DailyMetricsStore is empty")
		store.mu.Unlock()
		return 0
	}
	log.Println(time.Now().Format("15:04:05 ") + "Flushing DailyMetricsStore")

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
			stmt, err := Db.Prepare(sqlStr)
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
		stmt, _ := Db.Prepare(sqlStr)

		//format all vals at once
		stmt.Exec(vals...)
	}
	store.store = nil

	store.mu.Unlock()
	return len(vals)
}
