<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle;

use M6Web\Bundle\KafkaBundle\Exceptions\KafkaException;

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
     * @var \RdKafaka\Consumer
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
    public function setRdKafkaKafkaConsumer(\RdKafka\KafkaConsumer $kafkaConsumer)
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
     * @return \RdKafka\Message|string
     * @throws KafkaException
     */
    public function consume()
    {
        $message =  $this->rdKafkaKafkaConsumer->consume($this->timeoutConsumingQueue);

        if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
            $this->rdKafkaKafkaConsumer->commit($message);
            $this->notifyEvent($this->getOrigin());
        }

        return $message;
    }
}
