package database

import (
	transmitter "github.com/BrobridgeOrg/gravity-api/service/transmitter"
)

type Writer interface {
	Init() error
	ProcessData(*transmitter.Record) error
	Truncate(string) error
}
