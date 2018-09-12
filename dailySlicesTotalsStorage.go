package main

import (
	"log"
	"strconv"
	"sync"
	"time"
)

type DailySlicesTotalsStorage struct {
	mu    sync.Mutex
	store map[string]map[string]DailySlice
}

func (store *DailySlicesTotalsStorage) Inc(metricId int, sliceId int, event Event) bool {
	store.mu.Lock()
	var key string
	eventTime := time.Unix(event.Time, 0)
	eventTime, err := LocalTime(eventTime)
	if err != nil {
		log.Panic(err)
	}
	dateKey := eventTime.Format("2006_01_02")
	key = strconv.Itoa(metricId) + "_" + strconv.Itoa(sliceId)
	_, ok := store.store[dateKey]
	if ok {
		val, ok := store.store[dateKey][key]
		if ok {
			val.value = val.value + event.Value
			store.store[dateKey][key] = val
		} else {
			store.store[dateKey][key] = DailySlice{metricId: metricId, sliceId: sliceId, value: event.Value}
		}

	} else {
		store.store = make(map[string]map[string]DailySlice)
		_, ok := store.store[dateKey]
		if !ok {
			store.store[dateKey] = make(map[string]DailySlice)
		}
		store.store[dateKey][key] = DailySlice{metricId: metricId, sliceId: sliceId, value: event.Value}
	}
	store.mu.Unlock()
	return true
}

func (store *DailySlicesTotalsStorage) FlushToDb() int {
	store.mu.Lock()
	vals := []interface{}{}
	if store.store == nil {
		store.mu.Unlock()
		return 0
	}
	log.Println(time.Now().Format("15:04:05 ") + "Flushing DailySlicesTotals")

	tableCreated := false
	for dateKey, values := range store.store {
		tableName := "daily_slice_totals_" + dateKey
		//create table
		if !tableCreated {
			uniqueName := tableName + "_metric_id_slice_id_unique"
			//indexNameMinute := tableName + "_minute_index"
			sqlStr := "CREATE TABLE IF NOT EXISTS " + tableName +
				" (`id` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`metric_id` smallint(5) unsigned NOT NULL," +
				"`slice_id` smallint(5) unsigned NOT NULL," +
				"`value` int(11) NOT NULL," +
				"`diff` float NOT NULL DEFAULT '0'," +
				"PRIMARY KEY (`id`)," +
				"UNIQUE KEY " + uniqueName + " (`metric_id`,`slice_id`)" +
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
			Fields:    []string{"metric_id", "slice_id", "value"}}

		for _, dailySlice := range values {
			insertData.AppendValues(dailySlice.metricId, dailySlice.sliceId, dailySlice.value)
		}
		insertData.InsertIncrementBatch()

	}
	store.store = nil

	store.mu.Unlock()
	return len(vals)
}
