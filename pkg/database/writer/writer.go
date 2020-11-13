package writer

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	transmitter "github.com/BrobridgeOrg/gravity-api/service/transmitter"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-oci8"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	UpdateTemplate = `UPDATE %s SET %s WHERE %s = :primary_val`
	InsertTemplate = `INSERT INTO %s (%s) VALUES (%s)`
	DeleteTemplate = `DELETE FROM %s WHERE %s = :primary_val`
)

type DatabaseInfo struct {
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	ServiceName string `json:"service_name"`
	Param       string `json:"param"`
}

type RecordDef struct {
	HasPrimary    bool
	PrimaryColumn string
	Values        map[string]interface{}
	ColumnDefs    []*ColumnDef
}

type ColumnDef struct {
	ColumnName  string
	BindingName string
	Value       interface{}
}

type DBCommand struct {
	QueryStr string
	Args     map[string]interface{}
}

type Writer struct {
	dbInfo   *DatabaseInfo
	db       *sqlx.DB
	commands chan *DBCommand
}

func NewWriter() *Writer {
	return &Writer{
		dbInfo:   &DatabaseInfo{},
		commands: make(chan *DBCommand, 2048),
	}
}

func (writer *Writer) Init() error {

	// Read configuration file
	writer.dbInfo.Host = viper.GetString("database.host")
	writer.dbInfo.Port = viper.GetInt("database.port")
	writer.dbInfo.Username = viper.GetString("database.username")
	writer.dbInfo.Password = viper.GetString("database.password")
	writer.dbInfo.ServiceName = viper.GetString("database.service_name")
	writer.dbInfo.Param = viper.GetString("database.param")

	log.WithFields(log.Fields{
		"host":         writer.dbInfo.Host,
		"port":         writer.dbInfo.Port,
		"username":     writer.dbInfo.Username,
		"service_name": writer.dbInfo.ServiceName,
		"param":        writer.dbInfo.Param,
	}).Info("Connecting to database")

	connStr := fmt.Sprintf(
		"%s/%s@%s:%d/%s?%s",
		writer.dbInfo.Username,
		writer.dbInfo.Password,
		writer.dbInfo.Host,
		writer.dbInfo.Port,
		writer.dbInfo.ServiceName,
		writer.dbInfo.Param,
	)

	// Open database
	db, err := sqlx.Open("oci8", connStr)
	if err != nil {
		log.Error(err)
		return err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	writer.db = db

	go writer.run()

	return nil
}

func (writer *Writer) run() {
	for {
		select {
		case cmd := <-writer.commands:
			_, err := writer.db.NamedExec(cmd.QueryStr, cmd.Args)
			if err != nil {
				log.Error(err)
			}
		}
	}
}

func (writer *Writer) ProcessData(record *transmitter.Record) error {

	log.WithFields(log.Fields{
		"method": record.Method,
		"event":  record.EventName,
		"table":  record.Table,
	}).Info("Write record")

	switch record.Method {
	case transmitter.Method_DELETE:
		return writer.DeleteRecord(record)
	case transmitter.Method_UPDATE:
		return writer.UpdateRecord(record)
	case transmitter.Method_INSERT:
		return writer.InsertRecord(record)
	}

	return nil
}

func (writer *Writer) GetValue(value *transmitter.Value) interface{} {

	switch value.Type {
	case transmitter.DataType_FLOAT64:
		return float64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_INT64:
		return int64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_UINT64:
		return uint64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_BOOLEAN:
		return int8(value.Value[0]) & 1
	case transmitter.DataType_STRING:
		return string(value.Value)
	}

	// binary
	return value.Value
}

func (writer *Writer) GetDefinition(record *transmitter.Record) *RecordDef {

	recordDef := &RecordDef{
		HasPrimary: false,
		Values:     make(map[string]interface{}),
		ColumnDefs: make([]*ColumnDef, 0, len(record.Fields)),
	}

	// Scanning fields
	for n, field := range record.Fields {

		value := writer.GetValue(field.Value)

		// Primary key
		if field.IsPrimary == true {
			recordDef.Values["primary_val"] = value
			recordDef.HasPrimary = true
			recordDef.PrimaryColumn = field.Name
			continue
		}

		// Generate binding name
		bindingName := fmt.Sprintf("val_%s", strconv.Itoa(n))
		recordDef.Values[bindingName] = value

		// Store definition
		recordDef.ColumnDefs = append(recordDef.ColumnDefs, &ColumnDef{
			ColumnName:  field.Name,
			Value:       field.Name,
			BindingName: bindingName,
		})
	}

	return recordDef
}

func (writer *Writer) InsertRecord(record *transmitter.Record) error {

	recordDef := writer.GetDefinition(record)

	return writer.insert(record.Table, recordDef)
}

func (writer *Writer) UpdateRecord(record *transmitter.Record) error {

	recordDef := writer.GetDefinition(record)

	// Ignore if no primary key
	if recordDef.HasPrimary == false {
		return nil
	}

	_, err := writer.update(record.Table, recordDef)
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) DeleteRecord(record *transmitter.Record) error {

	for _, field := range record.Fields {

		// Primary key
		if field.IsPrimary == true {

			value := writer.GetValue(field.Value)

			sqlStr := fmt.Sprintf(DeleteTemplate, record.Table, field.Name)

			writer.commands <- &DBCommand{
				QueryStr: sqlStr,
				Args: map[string]interface{}{
					"primary_val": value,
				},
			}

			break
		}
	}

	return nil
}

func (writer *Writer) update(table string, recordDef *RecordDef) (bool, error) {

	// Preparing SQL string
	updates := make([]string, 0, len(recordDef.ColumnDefs))
	for _, def := range recordDef.ColumnDefs {
		updates = append(updates, def.ColumnName+` = :`+def.BindingName)
	}

	updateStr := strings.Join(updates, ",")
	sqlStr := fmt.Sprintf(UpdateTemplate, table, updateStr, recordDef.PrimaryColumn)

	writer.commands <- &DBCommand{
		QueryStr: sqlStr,
		Args:     recordDef.Values,
	}

	return false, nil
}

func (writer *Writer) insert(table string, recordDef *RecordDef) error {

	paramLength := len(recordDef.ColumnDefs) + 1

	// Allocation
	colNames := make([]string, 0, paramLength)
	colNames = append(colNames, recordDef.PrimaryColumn)
	valNames := make([]string, 0, paramLength)
	valNames = append(valNames, ":primary_val")

	// Preparing columns and bindings
	for _, def := range recordDef.ColumnDefs {
		colNames = append(colNames, def.ColumnName)
		valNames = append(valNames, `:`+def.BindingName)
	}

	// Preparing SQL string to insert
	colsStr := strings.Join(colNames, ",")
	valsStr := strings.Join(valNames, ",")
	insertStr := fmt.Sprintf(InsertTemplate, table, colsStr, valsStr)

	//	database.db.NamedExec(insertStr, recordDef.Values)
	writer.commands <- &DBCommand{
		QueryStr: insertStr,
		Args:     recordDef.Values,
	}

	return nil
}
