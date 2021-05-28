package subscriber

type Subscriber interface {
	Init() error
	Run() error
}
