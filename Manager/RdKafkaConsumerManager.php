<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Manager;

use M6Web\Bundle\KafkaBundle\Exceptions\KafkaException;
use M6Web\Bundle\KafkaBundle\Helper\NotifyEventTrait;

/**
 * Class RdKafkaConsumerManager
 * @package M6Web\Bundle\KafkaBundle
 *
 * A class to consume messages with topics
 */
class RdKafkaConsumerManager
{
    use NotifyEventTrait;

    /**
     * @var \RdKafka\Message
     */
    protected $message;

    /**
     * @var \RdKafka\Consumer
     */
    protected $rdKafkaKafkaConsumer;

    /**
     * @var int
     */
    protected $timeoutConsumingQueue;

    /**
     * @return string
     */
    public function getOrigin(): string
    {
        return 'consumer';
    }

    /**
     * @param array $topicNames
     *
     * @return void
     */
    public function addTopic(array $topicNames)
    {
        $this->rdKafkaKafkaConsumer->subscribe($topicNames);
    }

    /**
     * @param \RdKafka\KafkaConsumer $kafkaConsumer
     */
    public function setConsumer(\RdKafka\KafkaConsumer $kafkaConsumer)
    {
        $this->rdKafkaKafkaConsumer = $kafkaConsumer;
    }

    /**
     * @param int $timeoutConsumingQueue
     */
    public function setTimeoutConsumingQueue(int $timeoutConsumingQueue)
    {
        $this->timeoutConsumingQueue = $timeoutConsumingQueue;
    }

    /**
     * @param bool $autoCommit
     * @return \RdKafka\Message
     *
     * @throws KafkaException
     */
    public function consume(bool $autoCommit = true)
    {
        $this->message =  $this->rdKafkaKafkaConsumer->consume($this->timeoutConsumingQueue);

        if ($this->message->err === RD_KAFKA_RESP_ERR_NO_ERROR && $autoCommit) {
            $this->commit();
        }

        return $this->message;
    }

    /**
     * @return void
     */
    public function commit()
    {
        $this->notifyEvent($this->getOrigin());
        $this->rdKafkaKafkaConsumer->commit($this->message);
    }
}
