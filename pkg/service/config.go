package service

type NatsStreamingConfig struct {
	URL           string
	ClientID      string
	ClusterID     string
	Subscriptions []SubscriptionConfig
}

type SubscriptionConfig struct {
	Name        string
	QueueGroup  string
	DurableName string
	Limit       int
	Timeout     int
}
