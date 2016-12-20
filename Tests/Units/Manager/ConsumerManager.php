<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Tests\Units\Manager;

use M6Web\Bundle\KafkaBundle\Event\KafkaEvent;
use M6Web\Bundle\KafkaBundle\Manager\ConsumerManager as Base;
use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;

/**
 * Class ConsumerManager
 * @package M6Web\Bundle\KafkaBundle\Tests\Units\Consumer
 *
 * A class to test the consumer manager
 */
class ConsumerManager extends BaseUnitTest
{
    /**
     * @return void
     */
    public function testShouldConsumeMessages()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                    ->isInstanceOf('\RdKafka\Message')
                ->integer(count($result))
                    ->isEqualTo(1)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withArguments('kafka.event', new KafkaEvent('consumer'))
                            ->once()
                ->mock($consumerMock)
                    ->call('commit')
                        ->withArguments($result)
                            ->once()
        ;
    }

    /**
     * @return void
     */
    public function testShouldCommitMessageAfterConsumingAndMessage()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume(false))
            ->and($consumer->commit())
            ->then
                ->object($result)
                    ->isInstanceOf('\RdKafka\Message')
                ->integer(count($result))
                    ->isEqualTo(1)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withArguments('kafka.event', new KafkaEvent('consumer'))
                            ->once()
                ->mock($consumerMock)
                    ->call('commit')
                        ->withArguments($result)
                            ->once()
        ;
    }

    /**
     * @return void
     */
    public function testShouldCommitMessageWhenConsumingMessage()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume(false))
            ->then
                ->object($result)
                    ->isInstanceOf('\RdKafka\Message')
                ->integer(count($result))
                    ->isEqualTo(1)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->never()
                ->mock($consumerMock)
                    ->call('commit')
                        ->never()
        ;
    }

    /**
     * @return void
     */
    public function testShouldInformThatTheresNoMoreMessage()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(RD_KAFKA_RESP_ERR__PARTITION_EOF), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                ->integer($result->err)
                    ->isEqualTo(RD_KAFKA_RESP_ERR__PARTITION_EOF)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->never()
                ->mock($consumerMock)
                    ->call('commit')
                        ->never()
        ;
    }

    /**
     * @return void
     */
    public function testShouldInformThatTheresATimeOut()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(RD_KAFKA_RESP_ERR__TIMED_OUT), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                ->integer($result->err)
                    ->isEqualTo(RD_KAFKA_RESP_ERR__TIMED_OUT)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->never()
                ->mock($consumerMock)
                    ->call('commit')
                        ->never()
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotCommitMessageNeitherSendEventIfError()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock('error'), true, $eventDispatcherMock)
            )
            ->if($consumer->consume())
            ->then
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->never()
                ->mock($consumerMock)
                    ->call('commit')
                        ->never()
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
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(), false, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                    ->isInstanceOf('\RdKafka\Message')
                ->integer(count($result))
                    ->isEqualTo(1)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->never()
                ->mock($consumerMock)
                    ->call('commit')
                        ->once()
        ;
    }

    /**
     * @param \RdKafka\KafkaConsumer $consumer
     * @param bool                   $eventDispatcherSet
     * @param null                   $eventDispatcherMock
     * @return Base
     */
    protected function getReadyBase(\RdKafka\KafkaConsumer $consumer, bool $eventDispatcherSet = false, $eventDispatcherMock = null): Base
    {
        $consumerManager = new Base();
        $consumerManager->setConsumer($consumer);
        $consumerManager->addTopic(['name']);
        $consumerManager->setTimeoutConsumingQueue(1000);

        if ($eventDispatcherSet) {
            $consumerManager->setEventDispatcher($eventDispatcherMock);
        }

        return $consumerManager;
    }

    /**
     * @param string $topicName
     *
     * @return \mock\RdKafka\KafkaConsumer
     */
    protected function getConsumerMock($noError = true): \mock\RdKafka\KafkaConsumer
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\KafkaConsumer();
        $mock->getMockController()->consume = $this->getMessageMock($noError);

        return $mock;
    }
}
