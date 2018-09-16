package main

import (
	"log"
	"strconv"
	"sync"
	"time"
)

type DailyMetricsTotalsStorage struct {
	mu              sync.Mutex
	storageElements map[string]map[int]DailyMetric
	tmpMu sync.Mutex
	tmpStorageElements map[string]map[int]DailyMetric
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
	startTime:=time.Now()
	storage.mu.Lock()
	storage.tmpMu.Lock()
	storage.tmpStorageElements=storage.storageElements
	storage.storageElements = nil
	storage.mu.Unlock()

	vals := []interface{}{}
	if storage.tmpStorageElements == nil {
		//log.Println("DailyMetricsStore is empty")
		storage.tmpMu.Unlock()
		return 0
	}
	log.Println(time.Now().Format("15:04:05 ") + "Flushing DailyMetricsTotalsStorage")

	//monthly_metrics



	tableCreated := false
	for dateKey, values := range storage.tmpStorageElements {
		log.Println("DailyMetricsTotalsStorage dk("+strconv.Itoa(len(values))+")")
		tableName := "daily_metric_totals_" + dateKey
		//create table
		if !tableCreated {
			uniqueName := tableName + "_metric_id_unique"
			sqlStr := "CREATE TABLE IF NOT EXISTS " + tableName +
				" (`id` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`metric_id` smallint(5) unsigned NOT NULL," +
				"`value` int(11) unsigned NOT NULL," +
				"`diff` float NOT NULL DEFAULT '0'," +
				"PRIMARY KEY (`id`)," +
				"UNIQUE KEY " + uniqueName + " (`metric_id`)" +
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"
			//TODO: add error log here
			//CREATE TABLE `daily_slice_totals_2017_07_14` (
			//  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
			//  `metric_id` smallint(5) unsigned NOT NULL,
			//  `slice_id` smallint(5) unsigned NOT NULL,
			//  `value` int(11) NOT NULL,
			//  `diff` float NOT NULL DEFAULT '0',
			//  PRIMARY KEY (`id`),
			//  UNIQUE KEY `daily_slice_totals_2017_07_14_metric_id_slice_id_unique` (`metric_id`,`slice_id`)
			//) ENGINE=InnoDB AUTO_INCREMENT=34164 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
			stmt, err := Db.Prepare(sqlStr)
			if err != nil {
				log.Fatal(err)
			}
			stmt.Exec()
			stmt.Close()
			tableCreated = true
		}

		insertData := InsertData{
			TableName: tableName,
			Fields:    []string{"metric_id", "value"}}

		for _, dailyMetric := range values {
			insertData.AppendValues(dailyMetric.metricId, dailyMetric.value)
		}
		insertData.InsertIncrementBatch()

	}
	storage.tmpStorageElements = nil

	storage.tmpMu.Unlock()
	log.Println(time.Now().Format("15:04:05 ") + "Done Flushing DailyMetricsTotalsStorage. Elapsed:"+time.Since(startTime).String())
	return len(vals)
}
