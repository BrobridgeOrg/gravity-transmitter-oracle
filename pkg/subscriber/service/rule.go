package subscriber

type SubscriptionConfig map[string][]string

type RuleConfig struct {
	Subscriptions SubscriptionConfig `json:"subscriptions"`
}
