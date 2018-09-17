package main

import (
	"log"
	"strconv"
	"sync"
	"time"
)

type DailySlice struct {
	metricId  int
	sliceId   int
	value     int
	minute    int
	timestamp int64
}

type DailySlicesStorage struct {
	mu              sync.Mutex
	storageElements map[string]map[string]DailySlice
	tmpMu sync.Mutex
	tmpStorageElements map[string]map[string]DailySlice
}

func (storage *DailySlicesStorage) Lock(){
	storage.mu.Lock()
}

func (storage *DailySlicesStorage) Unlock(){
	storage.mu.Unlock()
}

func (storage *DailySlicesStorage) Inc(metricId int, sliceId int, event Event) bool {
	storage.mu.Lock()
	var key string
	eventTime := time.Unix(event.Time, 0)

	dateKey := eventTime.Format("2006_01_02")
	key = strconv.Itoa(metricId) + "_" + strconv.Itoa(sliceId) + "_" + strconv.Itoa(event.Minute)
	_, ok := storage.storageElements[dateKey]
	if ok {
		val, ok := storage.storageElements[dateKey][key]
		if ok {
			val.value = val.value + event.Value
			storage.storageElements[dateKey][key] = val
		} else {
			storage.storageElements[dateKey][key] = DailySlice{metricId: metricId, sliceId: sliceId, minute: event.Minute, value: event.Value, timestamp: event.Time}
		}

	} else {
		storage.storageElements = make(map[string]map[string]DailySlice)
		_, ok := storage.storageElements[dateKey]
		if !ok {
			storage.storageElements[dateKey] = make(map[string]DailySlice)
		}
		storage.storageElements[dateKey][key] = DailySlice{metricId: metricId, sliceId: sliceId, minute: event.Minute, value: event.Value, timestamp: event.Time}
	}
	storage.mu.Unlock()
	return true
}

func (storage *DailySlicesStorage) FlushToDb() int {
	startTime := time.Now()
	storage.mu.Lock()
	storage.tmpMu.Lock()
	storage.tmpStorageElements=storage.storageElements
	storage.storageElements = nil
	storage.mu.Unlock()

	vals := []interface{}{}
	if storage.tmpStorageElements == nil {
		storage.tmpMu.Unlock()
		return 0
	}
	log.Println(time.Now().Format("15:04:05 ") + "Flushing DailySlicesStorage")

	tableCreated := false
	for dateKey, values := range storage.tmpStorageElements {
		log.Println("DailySlicesStorage dk("+strconv.Itoa(len(values))+")")
		tableName := "daily_slices_" + dateKey
		//create table
		if !tableCreated {
			uniqueName := tableName + "_metric_id_slice_id_minute_unique"
			indexName := tableName + "_metric_id_slice_id_index"
			//indexNameMinute := tableName + "_minute_index"
			sqlStr := "CREATE TABLE IF NOT EXISTS " + tableName +
				" (`id` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`metric_id` smallint(5) unsigned NOT NULL," +
				"`slice_id` smallint(5) unsigned NOT NULL," +
				"`value` int(11) unsigned NOT NULL," +
				"`minute` smallint(5) unsigned NOT NULL," +
				"PRIMARY KEY (`id`)," +
				"UNIQUE KEY " + uniqueName + " (`metric_id`,`slice_id`,`minute`)," +
				"KEY " + indexName + " (`metric_id`, `slice_id`)" +
			//"KEY " + indexName + " (`minute`,)" +
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"

			stmt, err := Db.Prepare(sqlStr)
			if err != nil {
				log.Fatal(err)
			}
			_, err = stmt.Exec()
			if err != nil {
				log.Fatal(err)
			}
			stmt.Close()
			tableCreated = true
		}

		//insert rows
		insertData := InsertData{
			TableName: tableName,
			Fields:    []string{"metric_id", "slice_id", "value", "minute"}}
		for _, dailySlice := range values {
			insertData.AppendValues(dailySlice.metricId, dailySlice.sliceId, dailySlice.value, dailySlice.minute)
		}
		insertData.InsertIncrementBatch()

	}
	storage.tmpStorageElements = nil

	storage.tmpMu.Unlock()
	log.Println(time.Now().Format("15:04:05 ") + "Done Flushing DailySlicesStorage. Elapsed:"+time.Since(startTime).String())
	return len(vals)
}
