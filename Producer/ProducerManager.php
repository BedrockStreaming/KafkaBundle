<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Producer;

use M6Web\Bundle\KafkaBundle\AbstractManager;
use M6Web\Bundle\KafkaBundle\Exceptions\PartitionNotFoundException;

/**
 * Class Producer
 * @package M6Web\Bundle\KafkaBundle\Producer
 *
 * A class to produce messages
 */
class ProducerManager extends AbstractManager
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
        $this->checkIfEntitySet();

        $this->populateTopicsFromServer();

        array_walk($this->topics, $this->produceForEachTopic($message, $partition, $key));

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
            if ($partition !== -1 && !isset($this->topicsFromServer[$topic->getName()][$partition])) {
                throw new PartitionNotFoundException();
            }

            $topic->produce($partition, 0, $message);
        };
    }
}
