package transmitter

import (
	"crypto/tls"
	"net/http"

	"golang.org/x/net/context"

	pb "github.com/BrobridgeOrg/gravity-api/service/transmitter"
	app "github.com/BrobridgeOrg/gravity-transmitter-oracle/pkg/app"
)

var transport = &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

type Service struct {
	app app.App
}

func NewService(a app.App) *Service {

	service := &Service{
		app: a,
	}

	return service
}

func (service *Service) Send(ctx context.Context, in *pb.Record) (*pb.SendReply, error) {

	writer := service.app.GetWriter()
	err := writer.ProcessData(in)
	if err != nil {
		return &pb.SendReply{
			Success: false,
			Reason:  err.Error(),
		}, nil
	}

	return &pb.SendReply{
		Success: true,
	}, nil
}

func (service *Service) Truncate(ctx context.Context, in *pb.TruncateRequest) (*pb.TruncateReply, error) {

	writer := service.app.GetWriter()
	err := writer.Truncate(in.Table)
	if err != nil {
		return &pb.TruncateReply{
			Success: false,
			Reason:  err.Error(),
		}, nil
	}

	return &pb.TruncateReply{
		Success: true,
	}, nil
}
