package check

import (
	"math/rand"
	"time"

	"github.com/optiopay/kafka"
	log "github.com/sirupsen/logrus"
)

// HealthCheck holds all data required for health checking.
type HealthCheck struct {
	zookeeper              ZkConnection
	broker                 BrokerConnection
	consumer               kafka.Consumer
	producer               kafka.Producer
	config                 HealthCheckConfig
	partitionID            int32
	replicationPartitionID int32
	randSrc                rand.Source
}

// HealthCheckConfig is the configuration for the health check.
type HealthCheckConfig struct {
	MessageLength               int
	CheckInterval               time.Duration
	CheckTimeout                time.Duration
	DataWaitInterval            time.Duration
	NoTopicCreation             bool
	retryInterval               time.Duration
	topicName                   string
	replicationTopicName        string
	replicationFailureThreshold uint
	brokerHost                  string
	brokerID                    uint
	brokerPort                  uint
	zookeeperConnect            string
	statusServerPort            uint
}

type Update struct {
	Status string
	Data   []byte
}

// New creates a new health check with the given config.
func New(config HealthCheckConfig) *HealthCheck {
	return &HealthCheck{
		broker:    &kafkaBrokerConnection{},
		zookeeper: &zkConnection{},
		randSrc:   rand.NewSource(time.Now().UnixNano()),
		config:    config,
	}
}

// CheckHealth checks broker and cluster health.
func (check *HealthCheck) CheckHealth(brokerUpdates chan<- Update, clusterUpdates chan<- Update, stop <-chan struct{}) {
	manageTopic := !check.config.NoTopicCreation
	err := check.connect(manageTopic, stop)
	if err != nil {
		return
	}

	/**
	   	* 	Updated by Hussain:
	    *	TODO: Revisit to handle this properly
	    *   In our design, if the node is being terminated we don't really need to close the connection.
		* 	The closeConnection method deletes the health check topics . The deletion process actually informs the Zookeeper about it which
		* 	eventually deletes these topics. We have noticed during testing that the ASG terminates the node before it can even delete these topics from its
		* 	metadata. In our TA design, we attach the same EBS again to the new nodes. The zookeeper on these new nodes reuses the metadata from its previous life,
		* 	which contains information about the health check topics marked for deletion but not actually deleted. Due to this partially deleted state, health check process fails
		* 	to connect to its topics on the new healthy kafka nodes.
	*/

	// defer check.closeConnection(manageTopic)

	reportUnhealthy := func(err error) {
		log.Println("metadata could not be retrieved, assuming broker unhealthy:", err)
		brokerUpdates <- Update{unhealthy, simpleStatus(unhealthy)}
		clusterUpdates <- Update{red, simpleStatus(red)}
	}

	check.randSrc = rand.NewSource(time.Now().UnixNano())

	log.Info("starting health check loop")
	ticker := time.NewTicker(check.config.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			metadata, err := check.broker.Metadata()
			if err != nil {
				reportUnhealthy(err)
				continue
			}

			zkTopics, zkBrokers, err := check.getZooKeeperMetadata()
			if err != nil {
				reportUnhealthy(err)
				continue
			}

			brokerStatus := check.checkBrokerHealth(metadata)
			brokerUpdates <- newUpdate(brokerStatus, "broker")

			if brokerStatus.Status == unhealthy {
				clusterUpdates <- Update{red, simpleStatus(red)}
				log.Info("closing connection and reconnecting")
				if err := check.reconnect(stop); err != nil {
					log.Info("error while reconnecting:", err)
					return
				}
				log.Info("reconnected")
			} else {
				clusterStatus := check.checkClusterHealth(metadata, zkTopics, zkBrokers)
				clusterUpdates <- newUpdate(clusterStatus, "cluster")
			}
		case <-stop:
			return
		}
	}
}

func newUpdate(report StatusReport, name string) Update {
	data, err := report.Json()
	if err != nil {
		log.Warn("Error while marshaling %s status: %s", name, err.Error())
		data = simpleStatus(report.Summary())
	}
	return Update{report.Summary(), data}
}

func (check *HealthCheck) brokerConfig() kafka.BrokerConf {
	config := kafka.NewBrokerConf("health-check-client")
	config.DialRetryLimit = 1
	config.DialRetryWait = check.config.CheckTimeout
	return config
}

func (check *HealthCheck) consumerConfig() kafka.ConsumerConf {
	config := kafka.NewConsumerConf(check.config.topicName, 0)
	config.StartOffset = kafka.StartOffsetNewest
	config.RequestTimeout = check.config.CheckTimeout
	config.RetryLimit = 1
	config.RetryWait = check.config.CheckTimeout
	return config
}

func (check *HealthCheck) producerConfig() kafka.ProducerConf {
	config := kafka.NewProducerConf()
	config.RequestTimeout = check.config.CheckTimeout
	config.RetryLimit = 1
	return config
}
