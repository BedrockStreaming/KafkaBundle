<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Tests\Units\Manager;

use M6Web\Bundle\KafkaBundle\Event\KafkaEvent;
use M6Web\Bundle\KafkaBundle\Manager\RdKafkaConsumerManager as Base;
use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;

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
                $consumer = $this->getReadyBase($rdKafkaConsumerMock = $this->getRdKafkaConsumerMock(), true, $eventDispatcherMock)
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
                ->mock($rdKafkaConsumerMock)
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
                $consumer = $this->getReadyBase($rdKafkaConsumerMock = $this->getRdKafkaConsumerMock(), true, $eventDispatcherMock)
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
                ->mock($rdKafkaConsumerMock)
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
                $consumer = $this->getReadyBase($rdKafkaConsumerMock = $this->getRdKafkaConsumerMock(), true, $eventDispatcherMock)
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
                ->mock($rdKafkaConsumerMock)
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
                $consumer = $this->getReadyBase($rdKafkaConsumerMock = $this->getRdKafkaConsumerMock(RD_KAFKA_RESP_ERR__PARTITION_EOF), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                ->integer($result->err)
                    ->isEqualTo(RD_KAFKA_RESP_ERR__PARTITION_EOF)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->never()
                ->mock($rdKafkaConsumerMock)
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
                $consumer = $this->getReadyBase($rdKafkaConsumerMock = $this->getRdKafkaConsumerMock(RD_KAFKA_RESP_ERR__TIMED_OUT), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                ->integer($result->err)
                    ->isEqualTo(RD_KAFKA_RESP_ERR__TIMED_OUT)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->never()
                ->mock($rdKafkaConsumerMock)
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
                $consumer = $this->getReadyBase($rdKafkaConsumerMock = $this->getRdKafkaConsumerMock('error'), true, $eventDispatcherMock)
            )
            ->if($consumer->consume())
            ->then
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->never()
                ->mock($rdKafkaConsumerMock)
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
                $consumer = $this->getReadyBase($rdKafkaConsumerMock = $this->getRdKafkaConsumerMock(), false, $eventDispatcherMock)
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
                ->mock($rdKafkaConsumerMock)
                    ->call('commit')
                        ->once()
        ;
    }

    /**
     * @param \RdKafka\KafkaConsumer $rdKafkaConsumer
     * @param bool                   $eventDispatcherSet
     * @param null                   $eventDispatcherMock
     * @return Base
     */
    protected function getReadyBase(\RdKafka\KafkaConsumer $rdKafkaConsumer, bool $eventDispatcherSet = false, $eventDispatcherMock = null): Base
    {
        $consumer = new Base();
        $consumer->setConsumer($rdKafkaConsumer);
        $consumer->addTopic(['name']);
        $consumer->setTimeoutConsumingQueue(1000);

        if ($eventDispatcherSet) {
            $consumer->setEventDispatcher($eventDispatcherMock);
        }

        return $consumer;
    }

    /**
     * @param string $topicName
     *
     * @return \mock\RdKafka\KafkaConsumer
     */
    protected function getRdKafkaConsumerMock($noError = true): \mock\RdKafka\KafkaConsumer
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\KafkaConsumer();
        $mock->getMockController()->consume = $this->getMessageMock($noError);

        return $mock;
    }
}
