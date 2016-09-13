<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Consumer;

use M6Web\Bundle\KafkaBundle\Exceptions\BadUsageOfConsumerWithSeveralTopicsException;
use M6Web\Bundle\KafkaBundle\Exceptions\PartitionForTopicNotFoundException;
use M6Web\Bundle\KafkaBundle\Exceptions\TopicNotFoundException;
use M6Web\Bundle\KafkaBundle\Exceptions\TopicNotSetException;

/**
 * Class TopicsConsumptionState
 * @package M6Web\Bundle\KafkaBundle\Consumer
 *
 * A class to store and manipulate the topics consumption state
 */
class TopicsConsumptionState
{
    /**
     * @var array
     */
    protected $topics = [];

    /**
     * @var array
     */
    protected $topicsFromServer = [];

    /**
     * @param array $topicsFromServer
     */
    public function setTopicsFromServer(array $topicsFromServer = [])
    {
        $this->topicsFromServer = $topicsFromServer;
    }

    /**
     * @param string $topicName
     * @param bool   $defaultValuesFromServer
     */
    public function defineTopic(string $topicName, bool $defaultValuesFromServer = false)
    {
        if (!isset($this->topicsFromServer[$topicName])) {
            throw new TopicNotFoundException(sprintf('Topic "%s" does not exist', $topicName));
        }

        $this->topics[$topicName] = [];

        if ($defaultValuesFromServer) {
            foreach ($this->topicsFromServer[$topicName] as $partitionId) {
                $this->defineAPartitionForATopic($topicName, $partitionId);
            }
        }
    }

    /**
     * @return array
     */
    public function getTopics(): array
    {
        return array_keys($this->topics);
    }

    /**
     * @param string $topicName
     * @param int    $partitionId
     * @param int    $offset
     */
    public function defineAPartitionForATopic(string $topicName, int $partitionId, int $offset = \RD_KAFKA_OFFSET_BEGINNING)
    {
        if (!isset($this->topics[$topicName])) {
            throw new TopicNotSetException(sprintf('Topic "%s" not set', $topicName));
        }

        if (!in_array($partitionId, $this->topicsFromServer[$topicName])) {
            throw new PartitionForTopicNotFoundException(sprintf('Partition "%s" for topic "%s" does not exist', $partitionId, $topicName));
        }

        $this->topics[$topicName][$partitionId] = $offset;
    }

    /**
     * @param string $topicName
     * @return array
     */
    public function getPartitionsForATopic(string $topicName): array
    {
        return array_keys($this->topics[$topicName]);
    }

    /**
     * @param string $topicName
     * @param int    $partitionId
     * @return int
     */
    public function getOffsetForAPartitionForATopic(string $topicName, int $partitionId): int
    {
        return $this->topics[$topicName][$partitionId];
    }

    /**
     * @param int $partitionId
     * @param int $offset
     */
    public function defineAPartitionForAUniqueTopicSet(int $partitionId, int $offset = \RD_KAFKA_OFFSET_BEGINNING)
    {
        $this->checkIfConsumerGetsServeralTopics();

        $topicName = $this->getTopicNameWhenOnlyOneTopic();
        $this->topics[$topicName][$partitionId] = $offset;
    }

    /**
     * @param int $partitionId
     * @return mixed
     */
    public function getOffsetForAPartitionForAUniqueTopicSet(int $partitionId): int
    {
        $this->checkIfConsumerGetsServeralTopics();

        $topicName = $this->getTopicNameWhenOnlyOneTopic();

        if (!isset($this->topics[$topicName][$partitionId])) {
            throw new PartitionForTopicNotFoundException(sprintf('Partition "%s" for topic "%s" does not exist', $partitionId, $topicName));
        }

        return $this->topics[$topicName][$partitionId];
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function getPartitionsForAUniqueTopicSet(): array
    {
        $this->checkIfConsumerGetsServeralTopics();

        return array_keys($this->topics[$this->getTopicNameWhenOnlyOneTopic()]);
    }

    /**
     * @throws \Exception
     */
    protected function checkIfConsumerGetsServeralTopics()
    {
        if (count($this->topics) > 1) {
            throw new BadUsageOfConsumerWithSeveralTopicsException();
        }
    }

    /**
     * @return string
     */
    protected function getTopicNameWhenOnlyOneTopic()
    {
        return array_keys($this->topics)[0];
    }
}
