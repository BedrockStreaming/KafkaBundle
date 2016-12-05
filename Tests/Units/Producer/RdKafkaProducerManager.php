<?php
namespace M6Web\Bundle\KafkaBundle\Tests\Units\Producer;

use M6Web\Bundle\KafkaBundle\Event\EventLog;
use M6Web\Bundle\KafkaBundle\Producer\RdKafkaProducerManager as Base;
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
                $producer = $this->getReadyProducer()
            )
            ->if(
                $return = $producer->produce('message')
            )
            ->then
                ->variable($return)
                    ->isNull()
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
                $producer = $this->getReadyProducer(false),
                $producer->setEventDispatcher($eventDispatcherMock)
            )
            ->exception(function () use ($producer) {
                $producer->produce('message', 58);
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
                $producer = $this->getReadyProducer()
            )
            ->if(
                $return = $producer->produce('message', RD_KAFKA_PARTITION_UA, '12345')
            )
            ->then
                ->variable($return)
                    ->isNull()
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
                $producer = $this->getReadyProducer(),
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
                $producer = $this->getReadyProducer()
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
                $producer->setEntity($this->getRdKafkaProducerMock())
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
                $producer->setEntity($this->getRdKafkaProducerMock()),
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
    protected function getRdKafkaProducerMock($resultForProducing = true)
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Producer();
        $mock->getMockController()->newTopic = $this->getTopicMock('', $resultForProducing);
        $mock->getMockController()->getMetadata = $this->getMetadataMock('topicName');

        return $mock;
    }

    /**
     * @return Base
     */
    protected function getReadyProducer(bool $resultForProducing = true): Base
    {
        $producer = new Base();
        $producer->setEntity($this->getRdKafkaProducerMock($resultForProducing));
        $producer->addBrokers('127.0.0.1');
        $producer->setLogLevel(3);
        $producer->addTopic('name', new \RdKafka\TopicConf(), ['auto.commit.interval.ms' => '1000']);

        return $producer;
    }
}
