package subscriber

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	gravity_subscriber "github.com/BrobridgeOrg/gravity-sdk/subscriber"
	gravity_state_store "github.com/BrobridgeOrg/gravity-sdk/subscriber/state_store"
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/BrobridgeOrg/gravity-transmitter-oracle/pkg/app"
	"github.com/BrobridgeOrg/gravity-transmitter-oracle/pkg/database"
	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Subscriber struct {
	app               app.App
	stateStore        *gravity_state_store.StateStore
	subscriber        *gravity_subscriber.Subscriber
	ruleConfig        *RuleConfig
	completionCounter map[*gravity_subscriber.Message]int
}

func NewSubscriber(a app.App) *Subscriber {
	return &Subscriber{
		app:               a,
		completionCounter: make(map[*gravity_subscriber.Message]int),
	}
}

func (subscriber *Subscriber) processData(msg *gravity_subscriber.Message) error {

	event := msg.Payload.(*gravity_subscriber.DataEvent)
	record := event.Payload

	// Getting tables for specific collection
	tables, ok := subscriber.ruleConfig.Subscriptions[record.Table]
	if !ok {
		// skip
		return nil
	}

	//	log.Info(string(msg.Event.Data))

	// Save record to each table
	writer := subscriber.app.GetWriter()
	for _, tableName := range tables {
		var rs gravity_sdk_types_record.Record
		copier.Copy(&rs, record)
		rs.Table = tableName

		// TODO: using batch mechanism to improve performance
		for {
			err := writer.ProcessData(msg, &rs, tables)
			if err == nil {
				break
			}

			log.Error(err)
		}
	}

	return nil
}

func (subscriber *Subscriber) LoadConfigFile(filename string) (*RuleConfig, error) {

	// Open and read config file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	// Parse config
	var config RuleConfig
	json.Unmarshal(byteValue, &config)

	return &config, nil
}

func (subscriber *Subscriber) Init() error {

	// Load rules
	ruleFile := viper.GetString("rules.subscription")

	log.WithFields(log.Fields{
		"ruleFile": ruleFile,
	}).Info("Loading rules...")

	ruleConfig, err := subscriber.LoadConfigFile(ruleFile)
	if err != nil {
		return err
	}

	subscriber.ruleConfig = ruleConfig

	// Load state
	err = subscriber.InitStateStore()
	if err != nil {
		return err
	}

	// Initializing writer
	writer := subscriber.app.GetWriter()
	writer.SetCompletionHandler(func(cmd database.DBCommand) {
		// Ack after writing to database
		ref := cmd.GetReference()
		msg := ref.(*gravity_subscriber.Message)

		tables := cmd.GetTables()

		subscriber.completionCounter[msg] += 1
		if subscriber.completionCounter[msg] == len(tables) {
			delete(subscriber.completionCounter, msg)
			msg.Ack()
		}
	})

	// Initializing gravity node information
	viper.SetDefault("gravity.domain", "gravity")
	domain := viper.GetString("gravity.domain")
	host := viper.GetString("gravity.host")

	log.WithFields(log.Fields{
		"host": host,
	}).Info("Initializing gravity subscriber")

	// Initializing gravity subscriber and connecting to server
	viper.SetDefault("subscriber.workerCount", 4)
	viper.SetDefault("subscriber.chunkSize", 2048)
	options := gravity_subscriber.NewOptions()
	options.Verbose = viper.GetBool("subscriber.verbose")
	options.Domain = domain
	options.StateStore = subscriber.stateStore
	options.WorkerCount = viper.GetInt("subscriber.workerCount")
	options.ChunkSize = viper.GetInt("subscriber.chunkSize")
	options.InitialLoad.Enabled = viper.GetBool("initialLoad.enabled")
	options.InitialLoad.OmittedCount = viper.GetUint64("initialLoad.omittedCount")

	// Loading access key
	viper.SetDefault("subscriber.appID", "anonymous")
	viper.SetDefault("subscriber.accessKey", "")
	options.Key = keyring.NewKey(viper.GetString("subscriber.appID"), viper.GetString("subscriber.accessKey"))

	subscriber.subscriber = gravity_subscriber.NewSubscriber(options)
	opts := core.NewOptions()
	err = subscriber.subscriber.Connect(host, opts)
	if err != nil {
		return err
	}

	// Setup data handler
	subscriber.subscriber.SetEventHandler(subscriber.eventHandler)
	subscriber.subscriber.SetSnapshotHandler(subscriber.snapshotHandler)

	// Register subscriber
	log.Info("Registering subscriber")
	subscriberID := viper.GetString("subscriber.subscriberID")
	subscriberName := viper.GetString("subscriber.subscriberName")
	err = subscriber.subscriber.Register(gravity_subscriber.SubscriberType_Transmitter, "oracle", subscriberID, subscriberName)
	if err != nil {
		return err
	}

	// Subscribe to collections
	err = subscriber.subscriber.SubscribeToCollections(subscriber.ruleConfig.Subscriptions)
	if err != nil {
		return err
	}

	// Subscribe to pipelines
	err = subscriber.initializePipelines()
	if err != nil {
		return err
	}

	return nil
}

func (subscriber *Subscriber) initializePipelines() error {

	// Subscribe to pipelines
	log.WithFields(log.Fields{}).Info("Subscribing to gravity pipelines...")
	viper.SetDefault("subscriber.pipelineStart", 0)
	viper.SetDefault("subscriber.pipelineEnd", -1)

	pipelineStart := viper.GetInt64("subscriber.pipelineStart")
	pipelineEnd := viper.GetInt64("subscriber.pipelineEnd")

	// Subscribe to all pipelines
	if pipelineStart == 0 && pipelineEnd == -1 {
		err := subscriber.subscriber.AddAllPipelines()
		if err != nil {
			return err
		}

		return nil
	}

	// Subscribe to pipelines in then range
	if pipelineStart < 0 {
		return fmt.Errorf("subscriber.pipelineStart should be higher than -1")
	}

	if pipelineStart > pipelineEnd {
		if pipelineEnd != -1 {
			return fmt.Errorf("subscriber.pipelineStart should be less than subscriber.pipelineEnd")
		}
	}

	count, err := subscriber.subscriber.GetPipelineCount()
	if err != nil {
		return err
	}

	if pipelineEnd == -1 {
		pipelineEnd = int64(count) - 1
	}

	pipelines := make([]uint64, 0, pipelineEnd-pipelineStart)
	for i := pipelineStart; i <= pipelineEnd; i++ {
		pipelines = append(pipelines, uint64(i))
	}

	err = subscriber.subscriber.SubscribeToPipelines(pipelines)
	if err != nil {
		return err
	}

	return nil
}

func (subscriber *Subscriber) eventHandler(msg *gravity_subscriber.Message) {

	err := subscriber.processData(msg)
	if err != nil {
		log.Error(err)
		return
	}
}

func (subscriber *Subscriber) snapshotHandler(msg *gravity_subscriber.Message) {

	event := msg.Payload.(*gravity_subscriber.SnapshotEvent)
	snapshotRecord := event.Payload

	// Getting tables for specific collection
	tables, ok := subscriber.ruleConfig.Subscriptions[event.Collection]
	if !ok {
		return
	}

	// Prepare record for database writer
	var record gravity_sdk_types_record.Record
	record.Method = gravity_sdk_types_record.Method_INSERT
	record.Fields = snapshotRecord.Payload.Map.Fields

	// Save record to each table
	writer := subscriber.app.GetWriter()
	for _, tableName := range tables {
		var rs gravity_sdk_types_record.Record
		copier.Copy(&rs, record)
		rs.Table = tableName

		// TODO: using batch mechanism to improve performance
		for {
			err := writer.ProcessData(msg, &rs, tables)
			if err == nil {
				break
			}

			log.Error(err)
		}
	}
}

func (subscriber *Subscriber) Run() error {

	subscriber.subscriber.Start()

	return nil
}
