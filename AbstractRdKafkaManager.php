<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle;

use M6Web\Bundle\KafkaBundle\Event\EventLog;
use M6Web\Bundle\KafkaBundle\Exceptions\EntityNotSetException;
use M6Web\Bundle\KafkaBundle\Exceptions\LogLevelNotSetException;
use M6Web\Bundle\KafkaBundle\Exceptions\NoBrokerSetException;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

/**
 * Class AbstractRdKafkaManager
 * @package M6Web\Bundle\KafkaBundle
 *
 * A class for producer and consumer managers
 */
abstract class AbstractRdKafkaManager
{
    /**
     * @var int
     */
    protected $logLevel;

    /**
     * @var string
     */
    protected $brokers;

    /**
     * @var array
     */
    protected $topicsFromServer;

    /**
     * @var bool|EventDispatcher
     */
    protected $eventDispatcher = false;

    /**
     * Set an event dispatcher to notify a consumer or a producer action
     *
     * @param EventDispatcherInterface $eventDispatcher The eventDispatcher object, which implements the notify method
     *
     * @return void
     */
    public function setEventDispatcher(EventDispatcherInterface $eventDispatcher)
    {
        $this->eventDispatcher = $eventDispatcher;
    }

    /**
     * @param string $origin origin (producer or consumer)
     *
     * @return void
     */
    public function notifyEvent(string $origin)
    {
        $this
            ->eventDispatcher
                ->dispatch('kafka.event', new EventLog($origin));
    }

    /**
     * @param \RdKfaka $entity
     *
     * @return AbstractRdKafkaManager
     */
    public function setEntity(\RdKafka $entity): AbstractRdKafkaManager
    {
        $this->entity = $entity;

        return $this;
    }

    /**
     * @param integer $logLevel
     *
     * @return AbstractRdKafkaManager
     */
    public function setLogLevel(int $logLevel): AbstractRdKafkaManager
    {
        $this->checkIfEntitySet();

        $this->entity->setLogLevel($logLevel);
        $this->logLevel = $logLevel;

        return $this;
    }

    /**
     * @param string $brokers
     *
     * @return AbstractRdKafkaManager
     */
    public function addBrokers(string $brokers): AbstractRdKafkaManager
    {
        $this->checkIfEntitySet();

        $this->entity->addBrokers($brokers);
        $this->brokers = $brokers;

        return $this;
    }

    /**
     * @param string             $name
     * @param \RdKafka\TopicConf $rdKafkaTopicConf
     * @param array|null         $confToSet
     *
     * @return void
     */
    public function addTopic(string $name, \RdKafka\TopicConf $rdKafkaTopicConf, array $confToSet = [])
    {
        $this->checkIfEntitySet();
        $this->checkIfBrokersSet();
        $this->checkIfLogLevelSet();

        $topicConf = new $rdKafkaTopicConf();

        array_walk($confToSet, function ($confValue, $confIndex) use ($topicConf) {
            $topicConf->set($confIndex, (string) $confValue);
        });

        $this->topics[] = $this->entity->newTopic($name, $topicConf);
    }

    /**
     * Populate topics from server
     * to be sure that topics (with their partitions) asked by the entity exist
     * before using them
     *
     * @return void
     */
    protected function populateTopicsFromServer()
    {
        if (is_null($metadata = $this->entity->getMetadata(true, null, 1000))) {
            return;
        }

        foreach ($metadata->getTopics() as $topic) {
            foreach ($topic->getPartitions() as $partition) {
                if (!is_null($partition->getId())) {
                    $this->topicsFromServer[$topic->getTopic()][] = $partition->getId();
                }
            }
        }
    }

    /**
     * @throws \Exception
     */
    protected function checkIfEntitySet()
    {
        if (is_null($this->entity)) {
            throw new EntityNotSetException();
        }
    }

    /**
     * @throws \Exception
     */
    protected function checkIfBrokersSet()
    {
        if (is_null($this->brokers)) {
            throw new NoBrokerSetException();
        }
    }

    /**
     * @throws \Exception
     */
    protected function checkIfLogLevelSet()
    {
        if (is_null($this->logLevel)) {
            throw new LogLevelNotSetException();
        }
    }
}
