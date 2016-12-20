<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Manager;

use M6Web\Bundle\KafkaBundle\Exceptions\KafkaException;
use M6Web\Bundle\KafkaBundle\Helper\NotifyEventTrait;

/**
 * Class ConsumerManager
 * @package M6Web\Bundle\KafkaBundle
 *
 * A class to consume messages with topics
 */
class ConsumerManager
{
    use NotifyEventTrait;

    /**
     * @var \RdKafka\Message
     */
    protected $message;

    /**
     * @var \RdKafka\Consumer
     */
    protected $consumer;

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
        $this->consumer->subscribe($topicNames);
    }

    /**
     * @param \RdKafka\KafkaConsumer $consumer
     */
    public function setConsumer(\RdKafka\KafkaConsumer $consumer)
    {
        $this->consumer = $consumer;
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
        $this->message =  $this->consumer->consume($this->timeoutConsumingQueue);

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
        $this->consumer->commit($this->message);
    }
}
