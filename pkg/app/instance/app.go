package instance

import (
	writer "github.com/BrobridgeOrg/gravity-transmitter-oracle/pkg/database/writer"
	grpc_server "github.com/BrobridgeOrg/gravity-transmitter-oracle/pkg/grpc_server/server"
	mux_manager "github.com/BrobridgeOrg/gravity-transmitter-oracle/pkg/mux_manager/manager"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done       chan bool
	muxManager *mux_manager.MuxManager
	grpcServer *grpc_server.Server
	writer     *writer.Writer
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
	a.muxManager = mux_manager.NewMuxManager(a)
	a.grpcServer = grpc_server.NewServer(a)
	a.writer = writer.NewWriter()

	// Initializing Writer
	err := a.initWriter()
	if err != nil {
		return err
	}

	a.initMuxManager()

	// Initializing GRPC server
	err = a.initGRPCServer()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	// GRPC
	go func() {
		err := a.runGRPCServer()
		if err != nil {
			log.Error(err)
		}
	}()

	err := a.runMuxManager()
	if err != nil {
		return err
	}

	<-a.done

	return nil
}
