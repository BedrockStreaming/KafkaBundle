<?php
namespace M6Web\Bundle\KafkaBundle\Tests\Units\Consumer;

use M6Web\Bundle\KafkaBundle\Consumer\TopicsConsumptionState as Base;
use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;

/**
 * Class ConsumerManager
 * @package M6Web\Bundle\KafkaBundle\Tests\Units\Consumer
 *
 * A class to test the topics consumption state
 */
class TopicsConsumptionState extends BaseUnitTest
{
    /**
     * @return void
     */
    public function testShouldDefineTopic()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
            'topicName2' => [2, 3, 4],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer)
            )
            ->if(
                $topicsConsumptionState->defineTopic('topicName'),
                $topicsConsumptionState->defineTopic('topicName2')
            )
            ->then
                ->array($result = $topicsConsumptionState->getTopics())
                    ->isEqualTo(['topicName', 'topicName2'])
        ;
    }

    /**
     * @return void
     */
    public function testShouldDefineTopicsWithDefaultValues()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer)
            )
            ->if($topicsConsumptionState->defineTopic('topicName', true))
            ->then
                ->array($result = $topicsConsumptionState->getTopics())
                    ->isEqualTo(['topicName'])
                ->array($result = $topicsConsumptionState->getPartitionsForATopic('topicName'))
                    ->isEqualTo([1, 2, 3])
                ->integer($result = $topicsConsumptionState->getOffsetForAPartitionForATopic('topicName', 1))
                    ->isEqualTo(\RD_KAFKA_OFFSET_BEGINNING)
        ;
    }

    /**
     * @return void
     */
    public function testShouldDefineAPartitionForATopic()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer),
                $topicsConsumptionState->defineTopic('topicName')
            )
            ->if($topicsConsumptionState->defineAPartitionForATopic('topicName', '1'))
            ->then
                ->array($result = $topicsConsumptionState->getPartitionsForATopic('topicName'))
                    ->isEqualTo([1])
                ->integer($result = $topicsConsumptionState->getOffsetForAPartitionForATopic('topicName', '1'))
                    ->isEqualTo(\RD_KAFKA_OFFSET_BEGINNING)
        ;
    }

    /**
     * @return void
     */
    public function testShouldDefineAPartitionForATopicWithASetOffset()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer),
                $topicsConsumptionState->defineTopic('topicName')
            )
            ->if($topicsConsumptionState->defineAPartitionForATopic('topicName', '1', '5'))
            ->then
                ->integer($result = $topicsConsumptionState->getOffsetForAPartitionForATopic('topicName', 1))
                    ->isEqualTo(5)
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotDefineAPartitionIfTopicNotSet()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer)
            )
            ->exception(function () use ($topicsConsumptionState) {
                $topicsConsumptionState->defineAPartitionForATopic('topicName', 1);
            })
                ->isInstanceOf('\Exception')
                    ->hasMessage('Topic "topicName" not set')
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotDefineATopicIfItDoesNotExistInServer()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer),
                $topicsConsumptionState->defineTopic('topicName')
            )
            ->exception(function () use ($topicsConsumptionState) {
                $topicsConsumptionState->defineAPartitionForATopic('topicName', 4);
            })
            ->isInstanceOf('\Exception')
                ->hasMessage('Partition "4" for topic "topicName" does not exist')
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotDefineAPartitionForATopicIfPartitionDoesNotExistInServer()
    {
        $this
            ->given(
                $topicsConsumptionState = new Base()
            )
            ->exception(function () use ($topicsConsumptionState) {
                $topicsConsumptionState->defineTopic('topicName');
            })
            ->isInstanceOf('\Exception')
            ->hasMessage('Topic "topicName" does not exist')
        ;
    }

    /**
     * @return void
     */
    public function testShouldDefinePartitionsWithoutSpecifyingTheTopic()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
            'topicName2' => [2, 3, 4],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer),
                $topicsConsumptionState->defineTopic('topicName')
            )
            ->if(
                $topicsConsumptionState->defineAPartitionForAUniqueTopicSet(1)
            )
            ->then
                ->array($result = $topicsConsumptionState->getTopics())
                    ->isEqualTo(['topicName'])
                ->array($result = $topicsConsumptionState->getPartitionsForATopic('topicName'))
                    ->isEqualTo([1])
                ->integer($result = $topicsConsumptionState->getOffsetForAPartitionForAUniqueTopicSet(1))
                    ->isEqualTo(\RD_KAFKA_OFFSET_BEGINNING)
        ;
    }

    /**
     * @return void
     */
    public function testShouldDefinePartitionsAndOffsetsWithoutSpecifyingTheTopic()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
            'topicName2' => [2, 3, 4],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer),
                $topicsConsumptionState->defineTopic('topicName')
            )
            ->if(
                $topicsConsumptionState->defineAPartitionForAUniqueTopicSet(1, 10)
            )
            ->then
                ->array($result = $topicsConsumptionState->getTopics())
                    ->isEqualTo(['topicName'])
                ->array($result = $topicsConsumptionState->getPartitionsForATopic('topicName'))
                    ->isEqualTo([1])
                ->array($result = $topicsConsumptionState->getPartitionsForAUniqueTopicSet())
                    ->isEqualTo([1])
                ->integer($result = $topicsConsumptionState->getOffsetForAPartitionForAUniqueTopicSet(1))
                    ->isEqualTo(10)
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotDefinePartitionsWithoutSpecifyingTheTopicIfMoreThanOneTopicSet()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
            'topicName2' => [2, 3, 4],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer),
                $topicsConsumptionState->defineTopic('topicName'),
                $topicsConsumptionState->defineTopic('topicName2')
            )
            ->exception(function () use ($topicsConsumptionState) {
                $topicsConsumptionState->defineAPartitionForAUniqueTopicSet(1, 10);
            })
                ->isInstanceOf('\Exception')
                    ->hasMessage('Consumer gets several topics. You must specify which one to use')
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotGetPartitionsWithoutSpecifyingTheTopicIfMoreThanOneTopicSet()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
            'topicName2' => [2, 3, 4],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer),
                $topicsConsumptionState->defineTopic('topicName'),
                $topicsConsumptionState->defineTopic('topicName2')
            )
            ->exception(function () use ($topicsConsumptionState) {
                $topicsConsumptionState->getPartitionsForAUniqueTopicSet();
            })
            ->isInstanceOf('\Exception')
                ->hasMessage('Consumer gets several topics. You must specify which one to use')
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotGetOffsetForAPartitionWithoutSpecifyingTheTopicIfMoreThanOneTopicSet()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
            'topicName2' => [2, 3, 4],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer),
                $topicsConsumptionState->defineTopic('topicName'),
                $topicsConsumptionState->defineTopic('topicName2')
            )
            ->exception(function () use ($topicsConsumptionState) {
                $topicsConsumptionState->getOffsetForAPartitionForAUniqueTopicSet(1);
            })
                ->isInstanceOf('\Exception')
                    ->hasMessage('Consumer gets several topics. You must specify which one to use')
        ;
    }

    /**
     * @return void
     */
    public function testShouldNotGetOffsetForAPartitionWithoutSpecifyingIfNotPartitionSet()
    {
        $topicsFromServer = [
            'topicName' => [1, 2, 3],
            'topicName2' => [2, 3, 4],
        ];

        $this
            ->given(
                $topicsConsumptionState = new Base(),
                $topicsConsumptionState->setTopicsFromServer($topicsFromServer),
                $topicsConsumptionState->defineTopic('topicName')
            )
            ->exception(function () use ($topicsConsumptionState) {
                $topicsConsumptionState->getOffsetForAPartitionForAUniqueTopicSet(1);
            })
                ->isInstanceOf('\Exception')
                    ->hasMessage('Partition "1" for topic "topicName" does not exist')
        ;
    }
}
