<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Factory;

use M6Web\Bundle\KafkaBundle\Helper\PartitionAssignment;

/**
 * Class AbstractKafkaFactory
 */
class AbstractKafkaFactory
{
    /**
     * @var \RdKafka\Conf
     */
    protected $configuration;

    /**
     * @var \RdKafka\TopicConf
     */
    protected $topicConfiguration;

    /**
     * AbstractKafkaFactory constructor.
     *
     * @param \RdKafka\Conf      $configuration
     * @param \RdKafka\TopicConf $topicConfiguration
     */
    public function __construct(\RdKafka\Conf $configuration, \RdKafka\TopicConf $topicConfiguration)
    {
        $this->configuration = $configuration;
        $this->topicConfiguration = $topicConfiguration;
    }

    /**
     * @param array $configurationToSet
     *
     * @return \RdKafka\Conf
     */
    protected function getReadyConfiguration(array $configurationToSet = [])
    {
        $revertConfigurationToSet = array_flip($configurationToSet);
        array_walk($revertConfigurationToSet, [$this->configuration, 'set']);

        // Set a rebalance callback to log automatically assign partitions
        $this->configuration->setRebalanceCb(PartitionAssignment::handlePartitionsAssignment());
    }

    /**
     * @param array $configurationToSet
     * @return \RdKafka\TopicConf
     */
    protected function getReadyTopicConf(array $configurationToSet = [])
    {
        $revertConfigurationToSet = array_flip($configurationToSet);
        array_walk($revertConfigurationToSet, [$this->topicConfiguration, 'set']);
    }
}
