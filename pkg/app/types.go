package app

import (
	"github.com/BrobridgeOrg/gravity-transmitter-oracle/pkg/database"
)

type App interface {
	GetWriter() database.Writer
}
