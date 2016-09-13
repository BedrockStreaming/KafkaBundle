<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Fixtures;

use M6Web\Bundle\KafkaBundle\AbstractManager;
use M6Web\Bundle\KafkaBundle\Consumer\ConsumerManager;
use M6Web\Bundle\KafkaBundle\Consumer\TopicsConsumptionState;
use M6Web\Bundle\KafkaBundle\Tests\Units\Producer\ProducerManager;

/**
 * Class ConsumerManagerStub
 * @package M6Web\Bundle\KafkaBundle\Fixtures
 *
 * A stub class to imitate ConsumerManager class
 */
class ConsumerManagerStub extends ConsumerManager
{
    /**
     * @param \RdKafka $entity
     * @return $this
     */
    public function setEntity(\RdKafka $entity): AbstractManager
    {
        return $this;
    }

    /**
     * @param string $brokers
     * @return $this
     */
    public function addBrokers(string $brokers): AbstractManager
    {
        return $this;
    }

    /**
     * @param int $logLevel
     * @return $this
     */
    public function setLogLevel(int $logLevel): AbstractManager
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
    public function defineTopicsConsumptionState(TopicsConsumptionState $topicsConsumptionState): ConsumerManager
    {
        return $this;
    }
}
