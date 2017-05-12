<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Manager;

use M6Web\Bundle\KafkaBundle\Exceptions\EntityNotSetException;
use M6Web\Bundle\KafkaBundle\Exceptions\KafkaException;
use M6Web\Bundle\KafkaBundle\Exceptions\LogLevelNotSetException;
use M6Web\Bundle\KafkaBundle\Exceptions\NoBrokerSetException;
use M6Web\Bundle\KafkaBundle\Exceptions\ProducerException;
use M6Web\Bundle\KafkaBundle\Helper\NotifyEventTrait;
use RdKafka\Topic;

/**
 * Class ProducerManager
 * @package M6Web\Bundle\KafkaBundle
 *
 * @package M6Web\Bundle\KafkaBundle
 */
class ProducerManager
{
    use NotifyEventTrait;

    /**
     * max time in second
     */
    const MAX_TIME_WAITING_QUEUE_EMPTY = 1;

    /**
     * @var \RdKafka\Producer
     */
    protected $producer;

    /**
     * @var \RdKafka\Topic[]
     */
    protected $topics = [];

    /**
     * @var int
     */
    protected $logLevel;

    /**
     * @var array
     */
    protected $brokers;

    /**
     * event poll timeout in ms
     * @var int
     */
    protected $eventsPollTimeout;

    /**
     * @return string
     */
    public function getOrigin(): string
    {
        return 'producer';
    }

    /**
     * @param \RdKafka\Producer $entity
     * @return ProducerManager
     */
    public function setProducer(\RdKafka\Producer $entity): self
    {
        $this->producer = $entity;

        return $this;
    }

    /**
     * @param int $logLevel
     * @return ProducerManager
     */
    public function setLogLevel(int $logLevel): self
    {
        $this->checkIfProducerSet();

        $this->producer->setLogLevel($logLevel);
        $this->logLevel = $logLevel;

        return $this;
    }

    /**
     * @param string $brokers
     * @return ProducerManager
     */
    public function addBrokers(string $brokers): self
    {
        $this->checkIfProducerSet();

        $this->producer->addBrokers($brokers);
        $this->brokers = $brokers;

        return $this;
    }

    /**
     * @param int $timeout ms
     *
     * @return ProducerManager
     */
    public function setEventsPollTimeout(int $timeout): self
    {
        $this->eventsPollTimeout = $timeout;

        return $this;
    }

    /**
     * @param string             $name
     * @param \RdKafka\TopicConf $topicConfiguration
     */
    public function addTopic(string $name, \RdKafka\TopicConf $topicConfiguration)
    {
        $this->checkIfProducerSet();
        $this->checkIfBrokersSet();
        $this->checkIfLogLevelSet();

        $this->topics[] = $this->producer->newTopic($name, $topicConfiguration);
    }

    /**
     * @param string      $message
     * @param string|null $key
     * @param integer     $partition
     *
     * @return void
     * @throws KafkaException
     */
    public function produce(string $message, string $key = null, int $partition = RD_KAFKA_PARTITION_UA)
    {
        try {
            array_walk($this->topics, $this->produceForEachTopic($message, $partition, $key));
            $startPoll = microtime(true);
            while ($this->producer->getOutQLen() > 0 && microtime(true) - $startPoll < self::MAX_TIME_WAITING_QUEUE_EMPTY) {
                $this->producer->poll($this->eventsPollTimeout);
            }
        } catch (\Exception $e) {
            throw new KafkaException($e->getMessage());
        }
    }

    /**
     * @param \RdKafka\Producer $kafka
     * @param \RdKafka\Message  $message
     *
     * @throws ProducerException
     */
    public function produceResponse(\RdKafka\Producer $kafka, \RdKafka\Message $message)
    {
        // @codingStandardsIgnoreStart
        $messageKey = md5($message->payload.$message->topic_name);
        // @codingStandardsIgnoreEnd

        if ($message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
            $this->notifyEvent($messageKey);

            return;
        }

        $this->notifyResponseErrorEvent($messageKey, $message->err);
    }

    /**
     * @param \RdKafka\Producer $kafka
     * @param int               $errorCode
     * @param string            $reason
     */
    public function produceError(\RdKafka\Producer $kafka, int $errorCode, string $reason)
    {
        $this->notifyErrorEvent($this->getOrigin(), $errorCode, $reason);
    }

    /**
     * @param string       $message
     * @param integer      $partition
     * @param string|null  $key
     *
     * @return callable
     */
    protected function produceForEachTopic(string $message, int $partition, string $key = null): callable
    {
        return function (Topic $topic) use ($message, $key, $partition) {
            $this->prepareEvent($this->getOrigin(), md5($message.$topic->getName()));
            /*The second argument is the msgflags. It must be 0 as seen in the documentation:
            https://arnaud-lb.github.io/php-rdkafka/phpdoc/rdkafka-producertopic.produce.html*/
            $topic->produce($partition, 0, $message, $key);
        };
    }

    /**
     * @throws EntityNotSetException
     */
    protected function checkIfProducerSet()
    {
        if (is_null($this->producer)) {
            throw new EntityNotSetException();
        }
    }

    /**
     * @throws NoBrokerSetException
     */
    protected function checkIfBrokersSet()
    {
        if (is_null($this->brokers)) {
            throw new NoBrokerSetException();
        }
    }

    /**
     * @throws LogLevelNotSetException
     */
    protected function checkIfLogLevelSet()
    {
        if (is_null($this->logLevel)) {
            throw new LogLevelNotSetException();
        }
    }
}
