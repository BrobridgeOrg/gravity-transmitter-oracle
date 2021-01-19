package writer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	transmitter "github.com/BrobridgeOrg/gravity-api/service/transmitter"
	"github.com/jmoiron/sqlx"
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
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	DbName   string `json:"db_name"`
	Param    string `json:"param"`
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
	dbInfo    *DatabaseInfo
	db        *sqlx.DB
	batchSize int
	timeout   time.Duration
	commands  chan *DBCommand
}

func NewWriter() *Writer {
	return &Writer{
		dbInfo:   &DatabaseInfo{},
		commands: make(chan *DBCommand, 204800),
	}
}

func (writer *Writer) Init() error {

	// Initialize data
	service_name := viper.GetString("database.service_name")
	sid := viper.GetString("database.sid")
	if service_name != "" && sid != "" {
		log.Error("Only one of service_name or sid can be used")
		return nil
	}

	dbname := ""
	if service_name != "" {
		dbname = service_name
	}

	if sid != "" {
		dbname = sid
	}

	// Read configuration file
	writer.dbInfo.Host = viper.GetString("database.host")
	writer.dbInfo.Port = viper.GetInt("database.port")
	writer.dbInfo.Username = viper.GetString("database.username")
	writer.dbInfo.Password = viper.GetString("database.password")
	writer.dbInfo.DbName = dbname
	writer.dbInfo.Param = viper.GetString("database.param")

	log.WithFields(log.Fields{
		"host":     writer.dbInfo.Host,
		"port":     writer.dbInfo.Port,
		"username": writer.dbInfo.Username,
		"dbname":   writer.dbInfo.DbName,
		"param":    writer.dbInfo.Param,
	}).Info("Connecting to database")

	connStr := fmt.Sprintf(
		"%s/%s@%s:%d/%s?%s",
		writer.dbInfo.Username,
		writer.dbInfo.Password,
		writer.dbInfo.Host,
		writer.dbInfo.Port,
		writer.dbInfo.DbName,
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

	writer.batchSize = viper.GetInt("config.max_batch_size")
	writer.timeout = viper.GetDuration("config.batch_timeout")

	for i := 0; i < 5; i++ {
		go writer.run()
	}
	return nil
}

func (writer *Writer) run() {

	var tx *sqlx.Tx
	batch := 0
	timer := time.NewTimer(0 * time.Millisecond)

	for {
		select {
		case cmd := <-writer.commands:

			if batch == 0 {
				tx = writer.db.MustBegin()
			}
			timer = time.NewTimer(writer.timeout * time.Millisecond)

			_, err := tx.NamedExec(cmd.QueryStr, cmd.Args)
			if err != nil {
				log.Error(err)
			}

			batch++

			if batch >= writer.batchSize {
				timer.Stop()
				err := tx.Commit()
				if err != nil {
					log.Error(err)
				}

				log.Info("Processing batch of ", batch)
				batch = 0
			}

		case <-timer.C:
			if batch > 0 {
				timer.Stop()
				err := tx.Commit()
				if err != nil {
					log.Error(err)
				}

				log.Info("Processing batch of ", batch)
				batch = 0
			}
		}
	}
}

func (writer *Writer) ProcessData(record *transmitter.Record) error {

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
		return math.Float64frombits(binary.LittleEndian.Uint64(value.Value))
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

func (writer *Writer) GetDefinition(record *transmitter.Record) (*RecordDef, error) {

	recordDef := &RecordDef{
		HasPrimary: false,
		Values:     make(map[string]interface{}),
		ColumnDefs: make([]*ColumnDef, 0, len(record.Fields)),
	}

	// Scanning fields
	for n, field := range record.Fields {

		value := writer.GetValue(field.Value)

		// Primary key
		//		if field.IsPrimary == true {
		if record.PrimaryKey == field.Name {
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

	if len(record.PrimaryKey) > 0 && !recordDef.HasPrimary {
		log.WithFields(log.Fields{
			"column": record.PrimaryKey,
		}).Error("Not found primary key")

		return nil, errors.New("Not found primary key")
	}

	return recordDef, nil
}

func (writer *Writer) InsertRecord(record *transmitter.Record) error {

	recordDef, err := writer.GetDefinition(record)
	if err != nil {
		return err
	}

	return writer.insert(record.Table, recordDef)
}

func (writer *Writer) UpdateRecord(record *transmitter.Record) error {

	recordDef, err := writer.GetDefinition(record)
	if err != nil {
		return err
	}

	// Ignore if no primary key
	if recordDef.HasPrimary == false {
		return nil
	}

	_, err = writer.update(record.Table, recordDef)
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) DeleteRecord(record *transmitter.Record) error {

	if record.PrimaryKey == "" {
		// Do nothing
		return nil
	}

	for _, field := range record.Fields {

		// Primary key
		//		if field.IsPrimary == true {
		if record.PrimaryKey == field.Name {

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
		updates = append(updates, "\""+def.ColumnName+"\" = :"+def.BindingName)
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
	paramLength := len(recordDef.ColumnDefs)
	if recordDef.HasPrimary {
		paramLength++
	}

	// Allocation
	colNames := make([]string, 0, paramLength)
	valNames := make([]string, 0, paramLength)

	if recordDef.HasPrimary {
		colNames = append(colNames, `"`+recordDef.PrimaryColumn+`"`)
		valNames = append(valNames, ":primary_val")
	}

	// Preparing columns and bindings
	for _, def := range recordDef.ColumnDefs {
		colNames = append(colNames, `"`+def.ColumnName+`"`)
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
