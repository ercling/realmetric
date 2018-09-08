package main

import (
	"log"
	"sync"
	"time"
)

type DailyMetricsTotalsStorage struct {
	mu    sync.Mutex
	storageElements map[string]map[int]DailyMetric
}

//TODO: try to refactor (add interface)
func (storage *DailyMetricsTotalsStorage) Inc(metricId int, event Event) bool {
	storage.mu.Lock()
	var key int
	eventTime := time.Unix(event.Time, 0)
	eventTime, err := LocalTime(eventTime)
	if err != nil {
		log.Panic(err)
	}
	dateKey := eventTime.Format("2006_01_02")
	key = metricId
	_, ok := storage.storageElements[dateKey]
	if ok {
		val, ok := storage.storageElements[dateKey][key]
		if ok {
			val.value = val.value + event.Value
			storage.storageElements[dateKey][key] = val
		} else {
			storage.storageElements[dateKey][key] = DailyMetric{metricId: metricId, value: event.Value}
		}

	} else {
		storage.storageElements = make(map[string]map[int]DailyMetric)
		_, ok := storage.storageElements[dateKey]
		if !ok {
			storage.storageElements[dateKey] = make(map[int]DailyMetric)
		}
		storage.storageElements[dateKey][key] = DailyMetric{metricId: metricId, value: event.Value}
	}
	storage.mu.Unlock()
	return true
}

func (storage *DailyMetricsTotalsStorage) FlushToDb() int {
	storage.mu.Lock()
	vals := []interface{}{}
	if storage.storageElements == nil {
		//log.Println("DailyMetricsStore is empty")
		storage.mu.Unlock()
		return 0
	}
	log.Println(time.Now().Format("15:04:05 ") + "Flushing DailyMetricsTotalsStorage")

	tableCreated := false
	for dateKey, values := range storage.storageElements {
		tableName := "daily_metric_totals_" + dateKey
		//create table
		if !tableCreated {
			uniqueName := tableName + "_metric_id_unique"
			sqlStr := "CREATE TABLE IF NOT EXISTS " + tableName +
				" (`id` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`metric_id` smallint(5) unsigned NOT NULL," +
				"`value` int(11) NOT NULL," +
				"PRIMARY KEY (`id`)," +
				"UNIQUE KEY " + uniqueName + " (`metric_id`)" +
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
		sqlStr := "INSERT INTO " + tableName + " (metric_id, value) VALUES "

		for _, dailyMetric := range values {
			sqlStr += "(?, ?),"
			vals = append(vals, dailyMetric.metricId, dailyMetric.value)
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
	storage.storageElements = nil

	storage.mu.Unlock()
	return len(vals)
}

