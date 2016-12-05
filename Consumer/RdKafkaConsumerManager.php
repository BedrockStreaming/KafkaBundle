<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Consumer;

use M6Web\Bundle\KafkaBundle\AbstractRdKafkaManager;
use M6Web\Bundle\KafkaBundle\Exceptions\NoConsumeStartLaunchException;
use M6Web\Bundle\KafkaBundle\Exceptions\NoTopicsConsumptionStateSetException;

/**
 * Class Consumer
 * @package M6Web\Bundle\KafkaBundle\Consumer
 *
 * A class to consume messages with topics set from the configuration or manually
 */
class RdKafkaConsumerManager extends AbstractRdKafkaManager
{
    const ORIGIN = 'consumer';

    /**
     * @var \RdKafaka\Consumer
     */
    protected $entity;

    /**
     * @var \RdKafka\Topics[]
     */
    protected $topics = [];

    /**
     * @var \RdKafka\Queue
     */
    protected $queue;

    /**
     * @var int
     */
    protected $timeoutConsumingQueue;

    /**
     * @var TopicsConsumptionState
     */
    protected $topicsConsumptionState;

    /**
     * @return TopicsConsumptionState|null
     */
    public function getTopicsConsumptionState()
    {
        return clone $this->topicsConsumptionState;
    }

    /**
     * @param TopicsConsumptionState $topicsConsumptionState
     */
    public function setTopicsConsumptionState(TopicsConsumptionState $topicsConsumptionState)
    {
        $this->topicsConsumptionState = $topicsConsumptionState;
    }

    /**
     * @param TopicsConsumptionState $topicsConsumptionState
     * @return $this
     * @throws \Exception
     */
    public function defineTopicsConsumptionState(TopicsConsumptionState $topicsConsumptionState): RdKafkaConsumerManager
    {
        $this->checkIfEntitySet();
        $this->checkIfBrokersSet();
        $this->checkIfLogLevelSet();

        $this->populateTopicsFromServer();
        $topicsConsumptionState->setTopicsFromServer($this->topicsFromServer);

        foreach ($this->topics as $topic) {
            $topicsConsumptionState->defineTopic($topic->getName(), true);
        }

        $this->topicsConsumptionState = $topicsConsumptionState;

        return $this;
    }

    /**
     * @param int $timeoutConsumingQueue
     *
     * @return $this
     */
    public function setTimeoutConsumingQueue(int $timeoutConsumingQueue)
    {
        $this->timeoutConsumingQueue = $timeoutConsumingQueue;

        return $this;
    }

    /**
     * @return array
     */
    public function consumeStart()
    {
        if (empty($this->topicsConsumptionState)) {
            throw new NoTopicsConsumptionStateSetException();
        }

        $this->queue = $this->entity->newQueue();

        $topics = $this->topicsConsumptionState->getTopics();
        array_walk($topics, $this->setQueue());
    }

    /**
     * @return \RdKafka\Message
     */
    public function consume(): \RdKafka\Message
    {
        if (is_null($this->queue)) {
            throw new NoConsumeStartLaunchException();
        }

        $msg = $this->queue->consume($this->timeoutConsumingQueue);

        $this->topicsConsumptionState->defineTopic($msg->topic_name);
        $this->topicsConsumptionState->defineAPartitionForATopic($msg->topic_name, $msg->partition, $msg->offset);

        if ($this->eventDispatcher) {
            $this->notifyEvent(self::ORIGIN);
        }

        return $msg;
    }

    /**
     * @return callable
     */
    protected function setQueue() : callable
    {
        return function ($topicName) {
            $newTopicToConsume = $this->entity->newTopic($topicName);

            $partitions = $this->topicsConsumptionState->getPartitionsForATopic($topicName);

            foreach ($partitions as $partitionNumber) {
                $offset = $this->topicsConsumptionState->getOffsetForAPartitionForATopic($topicName, $partitionNumber);

                $offsetToBegin = ((is_null($offset) || $offset) === \RD_KAFKA_OFFSET_BEGINNING) ? \RD_KAFKA_OFFSET_BEGINNING : $offset + 1;
                $newTopicToConsume->consumeQueueStart($partitionNumber, $offsetToBegin, $this->queue);
            }
        };
    }
}
