package main

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DailySlicesTotalsStorage struct {
	mu              sync.Mutex
	storageElements map[string]map[string]DailySlice
	tmpMu sync.Mutex
	tmpStorageElements map[string]map[string]DailySlice
}

func (storage *DailySlicesTotalsStorage) Inc(metricId int, sliceId int, event Event) bool {
	storage.mu.Lock()
	var key string
	eventTime := time.Unix(event.Time, 0)
	eventTime, err := LocalTime(eventTime)
	if err != nil {
		log.Panic(err)
	}
	dateKey := eventTime.Format("2006_01_02")
	key = strconv.Itoa(metricId) + "_" + strconv.Itoa(sliceId)
	_, ok := storage.storageElements[dateKey]
	if ok {
		val, ok := storage.storageElements[dateKey][key]
		if ok {
			val.value = val.value + event.Value
			storage.storageElements[dateKey][key] = val
		} else {
			storage.storageElements[dateKey][key] = DailySlice{metricId: metricId, sliceId: sliceId, value: event.Value}
		}

	} else {
		storage.storageElements = make(map[string]map[string]DailySlice)
		_, ok := storage.storageElements[dateKey]
		if !ok {
			storage.storageElements[dateKey] = make(map[string]DailySlice)
		}
		storage.storageElements[dateKey][key] = DailySlice{metricId: metricId, sliceId: sliceId, value: event.Value}
	}
	storage.mu.Unlock()
	return true
}

func (storage *DailySlicesTotalsStorage) FlushToDb() int {
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
	log.Println(time.Now().Format("15:04:05 ") + "Flushing DailySlicesTotals")

	tableCreated := false
	for dateKey, values := range storage.tmpStorageElements {
		log.Println("DailySlicesTotals dk("+strconv.Itoa(len(values))+")")
		date := strings.Replace(dateKey, "_", "-", -1)
		tableName := "daily_slice_totals_" + dateKey
		//create table
		if !tableCreated {
			uniqueName := tableName + "_metric_id_slice_id_unique"
			//indexNameMinute := tableName + "_minute_index"
			sqlStr := "CREATE TABLE IF NOT EXISTS " + tableName +
				" (`id` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`metric_id` smallint(5) unsigned NOT NULL," +
				"`slice_id` smallint(5) unsigned NOT NULL," +
				"`value` int(11) unsigned NOT NULL," +
				"`diff` float NOT NULL DEFAULT '0'," +
				"PRIMARY KEY (`id`)," +
				"UNIQUE KEY " + uniqueName + " (`metric_id`,`slice_id`)" +
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

		insertData := InsertData{
			TableName: tableName,
			Fields:    []string{"metric_id", "slice_id", "value"}}
		insertData2 := InsertData{
			TableName: "monthly_slices",
			Fields:    []string{"metric_id", "slice_id", "value", "date"}}

		for _, dailySlice := range values {
			insertData.AppendValues(dailySlice.metricId, dailySlice.sliceId, dailySlice.value)
			insertData2.AppendValues(dailySlice.metricId, dailySlice.sliceId, dailySlice.value, date)
		}
		insertData.InsertIncrementBatch()
		insertData2.InsertIncrementBatch()

	}
	storage.tmpStorageElements = nil

	storage.tmpMu.Unlock()
	log.Println(time.Now().Format("15:04:05 ") + "Done Flushing DailySlicesTotals. Elapsed:"+time.Since(startTime).String())
	return len(vals)
}
