package instance

import (
	writer "github.com/BrobridgeOrg/gravity-transmitter-oracle/pkg/database/writer"
	subscriber "github.com/BrobridgeOrg/gravity-transmitter-oracle/pkg/subscriber/service"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done       chan bool
	writer     *writer.Writer
	subscriber *subscriber.Subscriber
}

func NewAppInstance() *AppInstance {

	a := &AppInstance{
		done: make(chan bool),
	}

	return a
}

func (a *AppInstance) Init() error {

	log.Info("Starting application")

	// Initializing modules
	a.writer = writer.NewWriter()
	a.subscriber = subscriber.NewSubscriber(a)

	// Initializing Writer
	err := a.initWriter()
	if err != nil {
		return err
	}

	err = a.subscriber.Init()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	err := a.subscriber.Run()
	if err != nil {
		return err
	}

	<-a.done

	return nil
}
