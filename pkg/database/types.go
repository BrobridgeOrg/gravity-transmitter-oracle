package database

import (
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
)

type Writer interface {
	Init() error
	ProcessData(*gravity_sdk_types_record.Record) error
	Truncate(string) error
}
