<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Fixtures;

use M6Web\Bundle\KafkaBundle\AbstractManager;
use M6Web\Bundle\KafkaBundle\Producer\ProducerManager;

/**
 * Class ProducerManagerStub
 * @package M6Web\Bundle\KafkaBundle\Fixtures
 *
 * A stub class to imitate ProducerManager class
 */
class ProducerManagerStub extends ProducerManager
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
}
