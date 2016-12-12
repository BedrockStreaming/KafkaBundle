<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Tests\Units\Consumer;

use M6Web\Bundle\KafkaBundle\Event\EventLog;
use M6Web\Bundle\KafkaBundle\Consumer\RdKafkaConsumerManager as Base;
use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;
use M6Web\Bundle\KafkaBundle\Consumer\TopicsConsumptionState;
use Symfony\Component\EventDispatcher\EventDispatcher;

/**
 * Class RdKafkaConsumerManager
 * @package M6Web\Bundle\KafkaBundle\Tests\Units\Consumer
 *
 * A class to test the consumer manager
 */
class RdKafkaConsumerManager extends BaseUnitTest
{
    /**
     * @return void
     */
    public function testShouldConsumeMessages()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $topicsConsumptionStateMock = $this->getTopicsConsumptionStateMock(),
                $consumer = $this->getReadyConsumer($topicsConsumptionStateMock, true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                    ->isInstanceOf('\RdKafka\Message')
                    ->integer(count($result))
                        ->isEqualTo(1)
                ->object($topicsStates = $consumer->getTopicsConsumptionState())
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withArguments('kafka.event', new EventLog('consumer'))
                            ->exactly(1)
                ->mock($topicsConsumptionStateMock)
                    ->call('defineTopic')
                        ->twice()
                    ->call('defineAPartitionForATopic')
                        ->once()
            ;
    }

    /**
     * @return void
     */
    public function testShouldConsumeMessagesWithoutSendingEvent()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $topicsConsumptionStateMock = $this->getTopicsConsumptionStateMock(),
                $consumer = $this->getReadyConsumer($topicsConsumptionStateMock, false, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
                ->then
                    ->object($result)
                        ->isInstanceOf('\RdKafka\Message')
                    ->integer(count($result))
                        ->isEqualTo(1)
                    ->object($topicsStates = $consumer->getTopicsConsumptionState())
                    ->mock($eventDispatcherMock)
                        ->call('dispatch')
                            ->never()
                    ->mock($topicsConsumptionStateMock)
                        ->call('defineTopic')
                            ->twice()
                        ->call('defineAPartitionForATopic')
                            ->once()
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotConsumeMessagesIfNoConsumeStart()
    {
        $this
            ->given(
                $consumer = new Base(),
                $consumer->setEntity($this->getRdKafkaConsumerMock()),
                $consumer->addBrokers('brokers'),
                $consumer->setLogLevel(3),
                $consumer->addTopic('name', new \RdKafka\TopicConf(), ['auto.commit.interval.ms' => '1000']),
                $consumer->defineTopicsConsumptionState(new TopicsConsumptionState()),
                $consumer->setTimeoutConsumingQueue(1000)
            )
            ->exception(function () use ($consumer) {
                $consumer->consume();
            })
                ->isInstanceOf('\Exception')
                ->hasMessage('You should launch the "consume start" action')
        ;
    }

    /**
     * @return void
     */
    public function testShouldGetTopicsConsumptionState()
    {
        ;
        $this
            ->given(
                $topicsConsumptionStateMock = $this->getTopicsConsumptionStateMock(),
                $consumer = $this->getReadyConsumer($topicsConsumptionStateMock)
            )
            ->if($consumer->consume())
                ->then
                    ->object($consumer->getTopicsConsumptionState())
                        ->isInstanceOf(TopicsConsumptionState::class)
        ;
    }

    /**
     * @return void
     */
    public function testShouldSetConsumptionState()
    {
        $this
            ->given(
                $topicsConsumptionStateMock = $this->getTopicsConsumptionStateMock(),
                $consumer = $this->getReadyConsumer($topicsConsumptionStateMock),
                $topicsConsumptionState = $consumer->getTopicsConsumptionState()
            )
            ->if($consumer->setTopicsConsumptionState($topicsConsumptionState))
            ->then
                ->object($consumer->getTopicsConsumptionState())
                    ->isInstanceOf(TopicsConsumptionState::class)
        ;
    }

    /**
     * @return void
     */
    public function testShouldDefineTopicsConsumptionState()
    {
        $eventDispatcherMock = $this->getEventDispatcherMock();
        $topicsConsumptionStateMock = $this->getTopicsConsumptionStateMock();

        $this
            ->given(
                $consumer = new Base(),
                $consumer->setEntity($this->getRdKafkaConsumerMock('topicName')),
                $consumer->addBrokers('brokers'),
                $consumer->setLogLevel(3),
                $consumer->addTopic('name', new \RdKafka\TopicConf())
            )
            ->if($consumer->defineTopicsConsumptionState($topicsConsumptionStateMock))
                ->then
                    ->object($consumer->getTopicsConsumptionState())
                        ->isInstanceOf(TopicsConsumptionState::class)
                ->mock($topicsConsumptionStateMock)
                    ->call('defineTopic')
                        ->once()
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotDefineTopicsConsumptionStateIfNoEntity()
    {
        $this
            ->given(
                $consumer = new Base()
            )
            ->exception(function () use ($consumer) {
                $consumer->defineTopicsConsumptionState(new TopicsConsumptionState());
            })
                ->isInstanceOf('\Exception')
                ->hasMessage('Entity not set')
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotDefineTopicsConsumptionStateIfNoBrokers()
    {
        $this
            ->given(
                $consumer = new Base(),
                $consumer->setEntity($this->getRdKafkaConsumerMock('topicName'))
            )
            ->exception(function () use ($consumer) {
                $consumer->defineTopicsConsumptionState(new TopicsConsumptionState());
            })
                ->isInstanceOf('\Exception')
                ->hasMessage('No broker set')
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotDefineTopicsConsumptionStateIfNoLogLevel()
    {
        $this
            ->given(
                $consumer = new Base(),
                $consumer->setEntity($this->getRdKafkaConsumerMock('topicName')),
                $consumer->addBrokers('brokers')
            )
            ->exception(function () use ($consumer) {
                $consumer->defineTopicsConsumptionState(new TopicsConsumptionState());
            })
                ->isInstanceOf('\Exception')
                ->hasMessage('Log level not set')
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotConsumeStartIfNoTopicsConsumptionState()
    {
        $this
            ->given(
                $consumer = new Base(),
                $consumer->setEntity($this->getRdKafkaConsumerMock()),
                $consumer->addBrokers('127.0.0.1'),
                $consumer->setLogLevel(3)
            )
            ->exception(function () use ($consumer) {
                $consumer->consumeStart();
            })
                ->isInstanceOf('\Exception')
                ->hasMessage('No topics consumption state set')
        ;
    }

    /**
     * @return \mock\M6Web\Bundle\KafkaBundle\Consumer\TopicsConsumptionState
     */
    protected function getTopicsConsumptionStateMock(): \mock\M6Web\Bundle\KafkaBundle\Consumer\TopicsConsumptionState
    {
        $this->mockGenerator->orphanize('__construct');

        $mock = new \mock\M6Web\Bundle\KafkaBundle\Consumer\TopicsConsumptionState();
        $mock->getMockController()->defineTopic = true;
        $mock->getMockController()->defineAPartitionForATopic = true;

        return $mock;
    }

    /**
     * @param TopicsConsumptionState $topicsConsumptionStateMock
     * @param bool                   $eventDispatcherSet
     * @param EventDispatcher        $eventDispatcherMock
     * @return Base
     */
    protected function getReadyConsumer($topicsConsumptionStateMock = null, $eventDispatcherSet = false, $eventDispatcherMock = null): Base
    {
        $consumer = new Base();
        $consumer->setEntity($this->getRdKafkaConsumerMock());
        $consumer->addBrokers('brokers');
        $consumer->setLogLevel(3);
        $consumer->addTopic('name', new \RdKafka\TopicConf(), ['auto.commit.interval.ms' => '1000']);
        $consumer->defineTopicsConsumptionState($topicsConsumptionStateMock ?? new TopicsConsumptionState());
        $consumer->setTimeoutConsumingQueue(1000);
        $consumer->consumeStart();

        if ($eventDispatcherSet) {
            $consumer->setEventDispatcher($eventDispatcherMock);
        }

        return $consumer;
    }
}
