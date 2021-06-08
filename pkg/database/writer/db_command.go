package writer

import gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"

type DBCommand struct {
	PipelineID uint64
	Sequence   uint64
	Reference  interface{}
	Record     *gravity_sdk_types_record.Record
	QueryStr   string
	Args       map[string]interface{}
}

func (cmd *DBCommand) GetReference() interface{} {
	return cmd.Reference
}

func (cmd *DBCommand) GetPipelineID() uint64 {
	return cmd.PipelineID
}

func (cmd *DBCommand) GetSequence() uint64 {
	return cmd.Sequence
}
