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
     * @param array $name
     *
     * @return void
     */
    public function addTopic(array $name)
    {
        $this->rdKafkaKafkaConsumer->subscribe($name);
    }

    /**
     * @param \RdKafka\KafkaConsumer $kafkaConsumer
     * @return $this
     */
    public function setRdKafkaKafkaConsumer(\RdKafka\KafkaConsumer $kafkaConsumer)
    {
        $this->rdKafkaKafkaConsumer = $kafkaConsumer;
    }

    /**
     * @param int $timeoutConsumingQueue
     *
     * @return $this
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

        if ($message->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            return 'No more message';
        }

        if ($message->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
            return 'Time out';
        }

        if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
            $this->rdKafkaKafkaConsumer->commit();
            $this->notifyEvent($this->getOrigin());

            return $message;
        }

        throw new KafkaException($message->err);
    }
}
