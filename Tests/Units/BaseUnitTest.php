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
     * @return \mock\Symfony\Component\EventDispatcher\EventDispatcherInterface
     */
    protected function getEventDispatcherMock(): \mock\Symfony\Component\EventDispatcher\EventDispatcherInterface
    {
        $this->mockGenerator->orphanize('__construct');
        $this->getMockGenerator()->shuntParentClassCalls();

        $mock = new \mock\Symfony\Component\EventDispatcher\EventDispatcherInterface();

        return $mock;
    }

    /**
     * @return \mock\RdKafka\Message
     */
    protected function getMessageMock($noError): \mock\RdKafka\Message
    {
        $this->mockGenerator->orphanize('__construct');

        $mock = new \mock\RdKafka\Message();

        $mock->payload = 'message';
        $mock->topic_name = 'topicName';
        $mock->partition = 2;
        $mock->offset = 4;
        $mock->err = is_bool($noError) && $noError ? RD_KAFKA_RESP_ERR_NO_ERROR : $noError;

        return $mock;
    }
}
