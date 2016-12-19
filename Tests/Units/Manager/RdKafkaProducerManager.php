<?php
namespace M6Web\Bundle\KafkaBundle\Tests\Units\Manager;

use M6Web\Bundle\KafkaBundle\Event\EventLog;
use M6Web\Bundle\KafkaBundle\Manager\RdKafkaProducerManager as Base;
use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;

/**
 * Class RdKafkaProducerManager
 * @package M6Web\Bundle\KafkaBundle\Tests\Units\Producer
 *
 * A class to test the producer manager
 */
class RdKafkaProducerManager extends BaseUnitTest
{
    /**
     * @return void
     */
    public function testShouldProduceAMessage()
    {
        $this
            ->given(
                $producer = $this->getReadyBase(true, $topicMock = $this->getTopicMock())
            )
            ->if(
                $return = $producer->produce('message')
            )
            ->then
                ->variable($return)
                    ->isNull()
                ->mock($topicMock)
                    ->call('produce')
                        ->withArguments(RD_KAFKA_PARTITION_UA, 0, 'message')
                            ->once()
            ;
    }

    /**
     * @return void
     */
    public function testShouldNotProduceAMessageIfPartitionDoesNotExist()
    {
        $eventDispatcherMock = $this->getEventDispatcherMock();
        $this
            ->given(
                $producer = $this->getReadyBase(false),
                $producer->setEventDispatcher($eventDispatcherMock)
            )
            ->exception(function () use ($producer) {
                $producer->produce('message', null, 58);
            })
                ->isInstanceOf('M6Web\Bundle\KafkaBundle\Exceptions\KafkaException')
                ->hasMessage('Random error from Kafka itself')
            ->mock($eventDispatcherMock)
                ->call('dispatch')
                    ->withArguments('kafka.event', new EventLog('producer'))
                        ->never()
        ;
    }

    /**
     * @return void
     */
    public function testShouldProduceAMessageWithAKey()
    {
        $this
            ->given(
                $producer = $this->getReadyBase(true, $topicMock = $this->getTopicMock())
            )
            ->if(
                $return = $producer->produce('message', '12345')
            )
            ->then
                ->variable($return)
                    ->isNull()
                ->mock($topicMock)
                    ->call('produce')
                        ->withArguments(RD_KAFKA_PARTITION_UA, 0, 'message', '12345')
                            ->once()
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotifyAnEventWhenProducing()
    {
        $eventDispatcherMock = $this->getEventDispatcherMock();
        $this
            ->given(
                $producer = $this->getReadyBase(),
                $producer->setEventDispatcher($eventDispatcherMock)
            )
            ->if(
                $producer->produce('message')
            )
            ->then
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withArguments('kafka.event', new EventLog('producer'))
                            ->once()
            ;
    }

    /**
     * @return void
     */
    public function testShouldNotNotifyAnEventWhenProducingAndEventDispatcherSetToFalse()
    {
        $eventDispatcherMock = $this->getEventDispatcherMock();
        $this
            ->given(
                $producer = $this->getReadyBase()
            )
            ->if(
                $producer->produce('message')
            )
            ->then
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->never()
        ;
    }

    /**
     * @return void
     */
    public function testShouldThrowAnExceptionWhenSettingLogOnEmptyEntity()
    {
        $this
            ->given(
                $producer = new Base()
            )
            ->exception(function () use ($producer) {
                $producer->setLogLevel(2);
            })
                ->isInstanceOf('M6Web\Bundle\KafkaBundle\Exceptions\EntityNotSetException')
                ->hasMessage('Entity not set')
            ;
    }

    /**
     * @return void
     */
    public function testShouldThrowAnExceptionWhenAddingBrokersOnEmptyEntity()
    {
        $this
            ->given(
                $producer = new Base()
            )
            ->exception(function () use ($producer) {
                $producer->addBrokers('127.0.0.1');
            })
                ->isInstanceOf('M6Web\Bundle\KafkaBundle\Exceptions\EntityNotSetException')
                ->hasMessage('Entity not set')
        ;
    }

    /**
     * @return void
     */
    public function testShouldThrowAnExceptionWhenAddingTopicsOnEmptyEntity()
    {
        $this
            ->given(
                $producer = new Base()
            )
            ->exception(function () use ($producer) {
                $producer->addTopic('127.0.0.1', new \RdKafka\TopicConf());
            })
                ->isInstanceOf('M6Web\Bundle\KafkaBundle\Exceptions\EntityNotSetException')
                ->hasMessage('Entity not set')
        ;
    }

    /**
     * @return void
     */
    public function testShouldThrowAnExceptionWhenAddingTopicsWithNoBroker()
    {
        $this
            ->given(
                $producer = new Base(),
                $producer->setRdKafkaProducer($this->getRdKafkaProducerMock())
            )
            ->exception(function () use ($producer) {
                $producer->addTopic('topicName', new \RdKafka\TopicConf());
            })
                ->isInstanceOf('M6Web\Bundle\KafkaBundle\Exceptions\NoBrokerSetException')
                ->hasMessage('No broker set')
        ;
    }

    /**
     * @return void
     */
    public function testShouldThrowAnExceptionWhenAddingTopicsWithNoLogLevel()
    {
        $this
            ->given(
                $producer = new Base(),
                $producer->setRdKafkaProducer($this->getRdKafkaProducerMock()),
                $producer->addBrokers('127.0.0.1')
            )
            ->exception(function () use ($producer) {
                $producer->addTopic('topicName', new \RdKafka\TopicConf());
            })
                ->isInstanceOf('M6Web\Bundle\KafkaBundle\Exceptions\LogLevelNotSetException')
                ->hasMessage('Log level not set')
        ;
    }

    /**
     * @return \mock\RdKafka\Producer
     */
    protected function getRdKafkaProducerMock($resultForProducing = true, $topicMock = null)
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Producer();
        $mock->getMockController()->newTopic = $topicMock ?? $this->getTopicMock($resultForProducing);

        return $mock;
    }

    /**
     * @return Base
     */
    protected function getReadyBase(bool $resultForProducing = true, $topicMock = null): Base
    {
        $producer = new Base();
        $producer->setRdKafkaProducer($this->getRdKafkaProducerMock($resultForProducing, $topicMock));
        $producer->addBrokers('127.0.0.1');
        $producer->setLogLevel(3);
        $producer->addTopic('name', new \RdKafka\TopicConf(), ['auto.commit.interval.ms' => '1000']);

        return $producer;
    }

    /**
     * @param string $topicName
     *
     * @return \mock\RdKafka\Topic
     */
    protected function getTopicMock(bool $resultForProducing = true): \mock\RdKafka\Topic
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Topic();

        $mock->getMockController()->produce = $resultForProducing ? $resultForProducing : function () {
            throw new \Exception('Random error from Kafka itself');
        };

        return $mock;
    }
}
