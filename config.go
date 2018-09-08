package main

import (
	"github.com/yosuke-furukawa/json5/encoding/json5"
	"os"
)

type Config struct {
	Db                         DbConfig
	MetricNameValidationRegexp string
	SliceNameValidationRegexp  string
	Gin                        GinConfig
	FlushToDbInterval          int
	FlushTotalsInterval        int
}

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
	Mode     string
	Host     string
	Port     int
	User     string
	Password string
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
