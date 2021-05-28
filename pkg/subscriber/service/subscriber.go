package subscriber

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/BrobridgeOrg/gravity-sdk/core"
	gravity_subscriber "github.com/BrobridgeOrg/gravity-sdk/subscriber"
	gravity_state_store "github.com/BrobridgeOrg/gravity-sdk/subscriber/state_store"
	gravity_sdk_types_projection "github.com/BrobridgeOrg/gravity-sdk/types/projection"
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/BrobridgeOrg/gravity-transmitter-oracle/pkg/app"
	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var projectionPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_projection.Projection{}
	},
}

type Subscriber struct {
	app        app.App
	stateStore *gravity_state_store.StateStore
	subscriber *gravity_subscriber.Subscriber
	ruleConfig *RuleConfig
}

func NewSubscriber(a app.App) *Subscriber {
	return &Subscriber{
		app: a,
	}
}

func (subscriber *Subscriber) processData(msg *gravity_subscriber.Message) error {

	pj := projectionPool.Get().(*gravity_sdk_types_projection.Projection)
	defer projectionPool.Put(pj)

	// Parsing data
	err := gravity_sdk_types_projection.Unmarshal(msg.Event.Data, pj)
	if err != nil {
		return err
	}

	// Getting tables for specific collection
	tables, ok := subscriber.ruleConfig.Subscriptions[pj.Collection]
	if !ok {
		return err
	}

	// Convert projection to record
	record, err := pj.ToRecord()
	if err != nil {
		return err
	}

	// Save record to each table
	writer := subscriber.app.GetWriter()
	for _, tableName := range tables {
		var rs gravity_sdk_types_record.Record
		copier.Copy(&rs, record)
		rs.Table = tableName

		// TODO: using batch mechanism to improve performance
		for {
			err := writer.ProcessData(&rs)
			if err == nil {
				break
			}

			<-time.After(time.Second * 5)
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

	host := viper.GetString("gravity.host")

	log.WithFields(log.Fields{
		"host": host,
	}).Info("Initializing gravity subscriber")

	// Initializing gravity subscriber and connecting to server
	viper.SetDefault("subscriber.worker_count", 4)
	options := gravity_subscriber.NewOptions()
	options.Verbose = false
	options.StateStore = subscriber.stateStore
	options.WorkerCount = viper.GetInt("subscriber.worker_count")

	subscriber.subscriber = gravity_subscriber.NewSubscriber(options)
	opts := core.NewOptions()
	err = subscriber.subscriber.Connect(host, opts)
	if err != nil {
		return err
	}

	// Register subscriber
	log.Info("Registering subscriber")
	subscriberID := viper.GetString("subscriber.subscriber_id")
	subscriberName := viper.GetString("subscriber.subscriber_name")
	err = subscriber.subscriber.Register(gravity_subscriber.SubscriberType_Transmitter, "oracle", subscriberID, subscriberName)
	if err != nil {
		return err
	}

	// Subscribe to collections
	err = subscriber.subscriber.SubscribeToCollections(subscriber.ruleConfig.Subscriptions)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{}).Info("Subscribing to gravity pipelines...")
	err = subscriber.subscriber.AddAllPipelines()
	if err != nil {
		return err
	}

	return nil
}

func (subscriber *Subscriber) Run() error {

	log.WithFields(log.Fields{}).Info("Starting to fetch data from gravity...")
	_, err := subscriber.subscriber.Subscribe(func(msg *gravity_subscriber.Message) {

		err := subscriber.processData(msg)
		if err != nil {
			log.Error(err)
			return
		}

		msg.Ack()
	})
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}
