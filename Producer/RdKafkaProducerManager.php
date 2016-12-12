<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Producer;

use M6Web\Bundle\KafkaBundle\AbstractRdKafkaManager;
use M6Web\Bundle\KafkaBundle\Exceptions\KafkaException;
use M6Web\Bundle\KafkaBundle\Exceptions\PartitionNotFoundException;

/**
 * Class Producer
 * @package M6Web\Bundle\KafkaBundle\Producer
 *
 * A class to produce messages
 */
class RdKafkaProducerManager extends AbstractRdKafkaManager
{

    const ORIGIN = 'producer';

    /**
     * @var \RdKafka\Producer
     */
    protected $entity;

    /**
     * @var \RdKafka\Topic[]
     */
    protected $topics = [];

    /**
     * @param string       $message
     * @param integer      $partition
     * @param integer|null $key
     *
     * @return void
     */
    public function produce(string $message, int $partition = \RD_KAFKA_PARTITION_UA, string $key = null)
    {
        try {
            array_walk($this->topics, $this->produceForEachTopic($message, $partition, $key));
        } catch (\Exception $e) {
            throw new KafkaException($e->getMessage());
        }

        if ($this->eventDispatcher) {
            $this->notifyEvent(self::ORIGIN);
        }
    }

    /**
     * @param string       $message
     * @param integer      $partition
     * @param integer|null $key
     *
     * @return callable
     */
    protected function produceForEachTopic(string $message, int $partition, $key = null) : callable
    {
        return function ($topic) use ($message, $key, $partition) {
            $topic->produce($partition, 0, $message);
        };
    }
}
