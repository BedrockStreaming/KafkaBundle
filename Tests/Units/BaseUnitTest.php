<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Tests\Units;

use atoum\test;

/**
 * Class BaseUnitTest
 * @package M6Web\Bundle\KafkaBundle\Tests\Units
 */
class BaseUnitTest extends test
{
    /**
     * @return \mock\Symfony\Component\EventDispatcher\EventDispatcher
     */
    protected function getEventDispatcherMock()
    {
        $this->mockGenerator->orphanize('__construct');
        $this->getMockGenerator()->shuntParentClassCalls();

        $mock = new \mock\Symfony\Component\EventDispatcher\EventDispatcherInterface();

        return $mock;
    }

    /**
     * @param string $topicName
     *
     * @return \mock\RdKafka\Consumer
     */
    protected function getRdKafkaConsumerMock($topicName = 'test')
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Consumer();

        $mock->getMockController()->newQueue = $this->getQueueMock();
        $mock->getMockController()->newTopic = $this->getTopicMock();
        $mock->getMockController()->getMetadata = $this->getMetadataMock($topicName);

        return $mock;
    }

    /**
     * @return \mock\RdKafka\Metadata
     */
    protected function getMetadataMock($topicName): \mock\RdKafka\Metadata
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Metadata();

        $mock->getMockController()->getTopics = [$this->getTopicMock($topicName)];

        return $mock;
    }

    /**
     * @return \mock\RdKafka\Queue
     */
    protected function getQueueMock():  \mock\RdKafka\Queue
    {
        $this->mockGenerator->orphanize('__construct');

        $mock = new \mock\RdKafka\Queue();

        $mock->getMockController()->consume = $this->getMessageMock();

        return $mock;
    }

    /**
     * @return \mock\RdKafka\Message
     */
    protected function getMessageMock(): \mock\RdKafka\Message
    {
        $this->mockGenerator->orphanize('__construct');

        $mock = new \mock\RdKafka\Message();

        $mock->payload = 'message';
        $mock->topic_name = 'topicName';
        $mock->partition = 2;
        $mock->offset = 4;

        return $mock;
    }

    /**
     * @param string $topicName
     *
     * @return \mock\RdKafka\Topic
     */
    protected function getTopicMock($topicName = 'test', $resultForProducing = true): \mock\RdKafka\Topic
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Topic();

        $mock->getMockController()->getName = $topicName;
        $mock->getMockController()->getTopic = $topicName;
        $mock->getMockController()->produce = $resultForProducing ? $resultForProducing : function() {
            throw new \Exception('Random error from Kafka itself');
        };
        $mock->getMockController()->getPartitions = [$this->getPartitionMock()];

        return $mock;
    }


    /**
     * @return \mock\RdKafka\Partition
     */
    protected function getPartitionMock(): \mock\RdKafka\Partition
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Partition();

        $mock->getMockController()->getId = 1;

        return $mock;
    }
}
