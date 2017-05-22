<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Factory;

use M6Web\Bundle\KafkaBundle\Manager\ProducerManager;

/**
 * Class ProducerFactory
 */
class ProducerFactory extends AbstractKafkaFactory
{
    /**
     * @param string $producerClass
     * @param array  $producerData
     *
     * @return ProducerManager
     */
    public function get(string $producerClass, array $producerData): ProducerManager
    {
        $producerManager = new ProducerManager();
        $this->getReadyConfiguration($producerData['configuration']);

        $this->configuration->setDrMsgCb([$producerManager, 'produceResponse']);
        $this->configuration->setErrorCb([$producerManager, 'produceError']);

        $producer = new $producerClass($this->configuration);

        $producerManager->setProducer($producer);
        $producerManager
            ->addBrokers(implode(',', $producerData['brokers']))
            ->setLogLevel($producerData['log_level'])
            ->setEventsPollTimeout($producerData['events_poll_timeout'])
        ;

        foreach ($producerData['topics'] as $topicName => $topic) {
            $this->getReadyTopicConf($topic['configuration']);
            $this->topicConfiguration->setPartitioner((int) $topic['strategy_partition']);
            $producerManager->addTopic($topicName, $this->topicConfiguration);
        }

        return $producerManager;
    }
}
