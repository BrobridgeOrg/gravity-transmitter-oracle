package database

import (
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
)

type DBCommand interface {
	GetReference() interface{}
	GetPipelineID() uint64
	GetSequence() uint64
}

type CompletionHandler func(DBCommand)

type Writer interface {
	Init() error
	ProcessData(interface{}, *gravity_sdk_types_record.Record) error
	SetCompletionHandler(CompletionHandler)
	Truncate(string) error
}
