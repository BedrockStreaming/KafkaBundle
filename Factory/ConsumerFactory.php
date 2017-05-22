<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Factory;

use M6Web\Bundle\KafkaBundle\Manager\ConsumerManager;
use M6Web\Bundle\KafkaBundle\Helper\PartitionAssignment;

/**
 * Class ConsumerFactory
 */
class ConsumerFactory extends AbstractKafkaFactory
{
    /**
     * @param string $consumerClass
     * @param array  $consumerData
     *
     * @return ConsumerManager
     */
    public function get(string $consumerClass, array $consumerData): ConsumerManager
    {
        $consumerManager = new ConsumerManager();

        $this->getReadyTopicConf($consumerData['topicConfiguration']);
        $this->getReadyConfiguration($consumerData['configuration']);
        $this->configuration->setDefaultTopicConf($this->topicConfiguration);

        // Set a rebalance callback to log automatically assign partitions
        $this->configuration->setRebalanceCb(PartitionAssignment::handlePartitionsAssignment());

        $consumer = new $consumerClass($this->configuration);

        $consumerManager->setConsumer($consumer);
        $consumerManager->addTopic($consumerData['topics'], $consumer);
        $consumerManager->setTimeoutConsumingQueue((int) $consumerData['timeout_consuming_queue']);

        return $consumerManager;
    }
}
