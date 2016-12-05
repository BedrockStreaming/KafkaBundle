<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Fixtures;

use M6Web\Bundle\KafkaBundle\AbstractRdKafkaManager;
use M6Web\Bundle\KafkaBundle\Consumer\RdKafkaConsumerManager;
use M6Web\Bundle\KafkaBundle\Consumer\TopicsConsumptionState;

/**
 * Class RdKafkaConsumerManagerStub
 * @package M6Web\Bundle\KafkaBundle\Fixtures
 *
 * A stub class to imitate RdKafkaConsumerManager class
 */
class RdKafkaConsumerManagerStub extends RdKafkaConsumerManager
{
    /**
     * @param \RdKafka $entity
     * @return $this
     */
    public function setEntity(\RdKafka $entity): AbstractRdKafkaManager
    {
        return $this;
    }

    /**
     * @param string $brokers
     * @return $this
     */
    public function addBrokers(string $brokers): AbstractRdKafkaManager
    {
        return $this;
    }

    /**
     * @param int $logLevel
     * @return $this
     */
    public function setLogLevel(int $logLevel): AbstractRdKafkaManager
    {
        return $this;
    }

    /**
     * @param string             $name
     * @param \RdKafka\TopicConf $rdKafkaTopicConf
     * @param array|null         $confToSet
     * @return $this
     */
    public function addTopic(string $name, \RdKafka\TopicConf $rdKafkaTopicConf, array $confToSet = null)
    {
        return $this;
    }

    /**
     * @param TopicsConsumptionState $topicsConsumptionState
     * @return $this
     */
    public function defineTopicsConsumptionState(TopicsConsumptionState $topicsConsumptionState): RdKafkaConsumerManager
    {
        return $this;
    }
}
