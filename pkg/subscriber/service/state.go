package subscriber

import (
	gravity_state_store "github.com/BrobridgeOrg/gravity-sdk/subscriber/state_store"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func (subscriber *Subscriber) InitStateStore() error {

	storePath := viper.GetString("subscriber.stateStore")
	log.WithFields(log.Fields{
		"path": storePath,
	}).Info("Loading state...")

	// Initializing state store
	options := gravity_state_store.NewOptions()
	options.Core.StoreOptions.DatabasePath = storePath
	stateStore := gravity_state_store.NewStateStore(options)
	err := stateStore.Initialize()
	if err != nil {
		return nil
	}

	subscriber.stateStore = stateStore

	return nil
}
