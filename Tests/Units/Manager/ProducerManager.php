<?php
namespace M6Web\Bundle\KafkaBundle\Tests\Units\Manager;

use M6Web\Bundle\KafkaBundle\Event\KafkaEvent;
use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;

/**
 * Class ProducerManager
 * @package M6Web\Bundle\KafkaBundle\Tests\Units\Producer
 *
 * A class to test the producer manager
 */
class ProducerManager extends BaseUnitTest
{
    /**
     * @return void
     */
    public function testShouldProduceAMessage()
    {
        $this
            ->given(
                $this->newTestedInstance(),
                $this->getReadyBase($topicMock = $this->getTopicMock()),
                $return = $this->testedInstance->produce('message')
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
                $this->newTestedInstance(),
                $this->getReadyBase(),
                $this->testedInstance->setEventDispatcher($eventDispatcherMock),
                $this->testedInstance->produce('message', null, 58)
            )
            ->mock($eventDispatcherMock)
                ->call('dispatch')
                    ->withAtLeastArguments([KafkaEvent::EVENT_NAME])
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
                $this->newTestedInstance(),
                $this->getReadyBase($topicMock = $this->getTopicMock()),
                $return = $this->testedInstance->produce('message', '12345')
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
                $this->newTestedInstance(),
                $this->getReadyBase(),
                $this->testedInstance->setEventDispatcher($eventDispatcherMock),
                $this->testedInstance->produce('message'),
                $this->mockCallbackProduce()
            )
            ->then
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withAtLeastArguments([KafkaEvent::EVENT_NAME])
                            ->once()
            ;
    }

    /**
     * @return void
     */
    public function testShouldNotifyAnEventWhenProducingWithError()
    {
        $eventDispatcherMock = $this->getEventDispatcherMock();
        $this
            ->given(
                $this->newTestedInstance(),
                $this->getReadyBase(),
                $this->testedInstance->setEventDispatcher($eventDispatcherMock),
                $this->testedInstance->produce('message'),
                $this->mockCallbackProduce(false)
            )
            ->then
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withAtLeastArguments([KafkaEvent::EVENT_ERROR_NAME])
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
                $this->newTestedInstance(),
                $this->getReadyBase(),
                $this->testedInstance->produce('message'),
                $this->mockCallbackProduce()
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
            ->if($this->newTestedInstance())
            ->exception(function () {
                $this->testedInstance->setLogLevel(2);
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
            ->if($this->newTestedInstance())
            ->exception(function () {
                $this->testedInstance->addBrokers('127.0.0.1');
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
            ->if($this->newTestedInstance())
            ->exception(function () {
                $this->testedInstance->addTopic('127.0.0.1', new \RdKafka\TopicConf());
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
                $this->newTestedInstance(),
                $this->testedInstance->setProducer($this->getProducerMock())
            )
            ->exception(function () {
                $this->testedInstance->addTopic('topicName', new \RdKafka\TopicConf());
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
                $this->newTestedInstance(),
                $this->testedInstance->setProducer($this->getProducerMock()),
                $this->testedInstance->addBrokers('127.0.0.1')
            )
            ->exception(function () {
                $this->testedInstance->addTopic('topicName', new \RdKafka\TopicConf());
            })
                ->isInstanceOf('M6Web\Bundle\KafkaBundle\Exceptions\LogLevelNotSetException')
                ->hasMessage('Log level not set')
        ;
    }

    /**
     * @return \mock\RdKafka\Producer
     */
    protected function getProducerMock($topicMock = null)
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Producer();
        $mock->getMockController()->newTopic = $topicMock ?? $this->getTopicMock();

        return $mock;
    }

    /**
     * @param null $topicMock
     */
    protected function getReadyBase($topicMock = null)
    {
        $this->testedInstance->setProducer($this->getProducerMock($topicMock));
        $this->testedInstance->addBrokers('127.0.0.1');
        $this->testedInstance->setLogLevel(3);
        $this->testedInstance->addTopic('name', new \RdKafka\TopicConf(), ['auto.commit.interval.ms' => '1000']);
    }

    /**
     * @return \mock\RdKafka\Topic
     */
    protected function getTopicMock(): \mock\RdKafka\Topic
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Topic();
        $mock->getMockController()->produce = null;
        $mock->getMockController()->getName = 'super topic';

        return $mock;
    }

    /**
     * @param bool $resultForProducing
     */
    protected function mockCallbackProduce($resultForProducing = true)
    {
        $this->mockGenerator->shuntParentClassCalls();
        $kafka = new \mock\RdKafka\Producer();
        if ($resultForProducing) {
            $message = new \RdKafka\Message();
            $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;
            $message->payload = 'message';
            $message->topic_name = 'super topic';
            $this->testedInstance->produceResponse($kafka, $message);
        } else {
            $this->testedInstance->produceError($kafka, RD_KAFKA_RESP_ERR__FAIL, 'failed');
        }
    }
}
