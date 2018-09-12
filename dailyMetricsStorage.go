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
	mu              sync.Mutex
	storageElements map[string]map[string]DailyMetric
}

func (storage *DailyMetricsStorage) Inc(metricId int, event Event) bool {
	storage.mu.Lock()
	var key string
	eventTime := time.Unix(event.Time, 0)
	eventTime, err := LocalTime(eventTime)
	if err != nil {
		log.Panic(err)
	}
	dateKey := eventTime.Format("2006_01_02")
	key = strconv.Itoa(metricId) + "_" + strconv.Itoa(event.Minute)
	_, ok := storage.storageElements[dateKey]
	if ok {
		val, ok := storage.storageElements[dateKey][key]
		if ok {
			val.value = val.value + event.Value
			storage.storageElements[dateKey][key] = val
		} else {
			storage.storageElements[dateKey][key] = DailyMetric{metricId: metricId, minute: event.Minute, value: event.Value, timestamp: event.Time}
		}

	} else {
		storage.storageElements = make(map[string]map[string]DailyMetric)
		_, ok := storage.storageElements[dateKey]
		if !ok {
			storage.storageElements[dateKey] = make(map[string]DailyMetric)
		}
		storage.storageElements[dateKey][key] = DailyMetric{metricId: metricId, minute: event.Minute, value: event.Value, timestamp: event.Time}
	}
	storage.mu.Unlock()
	return true
}

func (storage *DailyMetricsStorage) FlushToDb() int {
	storage.mu.Lock()
	vals := []interface{}{}
	if storage.storageElements == nil {
		//log.Println("DailyMetricsStore is empty")
		storage.mu.Unlock()
		return 0
	}
	log.Println(time.Now().Format("15:04:05 ") + "Flushing DailyMetricsStorage")

	tableCreated := false
	for dateKey, values := range storage.storageElements {
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

		insertData := InsertData{
			TableName: tableName,
			Fields:    []string{"metric_id", "value", "minute"}}

		for _, dailyMetric := range values {
			insertData.AppendValues(dailyMetric.metricId, dailyMetric.value, dailyMetric.minute)
		}
		insertData.InsertIncrementBatch()
	}
	storage.storageElements = nil

	storage.mu.Unlock()
	return len(vals)
}
