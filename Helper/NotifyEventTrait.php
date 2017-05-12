<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Helper;

use M6Web\Bundle\KafkaBundle\Event\KafkaEvent;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

/**
 * Trait NotifyEventTrait
 * @package M6Web\Bundle\KafkaBundle
 *
 * A class to handle notifications
 */
trait NotifyEventTrait
{
    /**
     * @var bool|EventDispatcher
     */
    protected $eventDispatcher = false;

    /**
     * @var array
     */
    protected $events = [];

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
     * @param string $origin
     * @param string $key
     */
    public function prepareEvent(string $origin, string $key)
    {
        if ($this->eventDispatcher) {
            $event = new KafkaEvent($origin);
            $event->setExecutionStart();

            $this->events[$key] = $event;
        }
    }

    /**
     * @param string $key
     */
    public function notifyEvent(string $key)
    {
        $event = $this->getEvent($key);
        if ($this->eventDispatcher && $event) {
            $event->setExecutionStop();

            $this
                ->eventDispatcher
                ->dispatch(KafkaEvent::EVENT_NAME, $event);
        }
    }

    /**
     * @param string $key
     * @param int    $errorCode
     */
    public function notifyResponseErrorEvent(string $key, int $errorCode)
    {
        $event = $this->getEvent($key);
        if ($this->eventDispatcher && $event) {
            $event->setExecutionStop();
            $event->setErrorCode($errorCode);
            $event->setReason(rd_kafka_err2str($errorCode));

            $this
                ->eventDispatcher
                ->dispatch(KafkaEvent::EVENT_ERROR_NAME, $event);
        }
    }

    /**
     * @param string $origin
     * @param int    $errorCode
     * @param string $reason
     */
    public function notifyErrorEvent(string $origin, int $errorCode, string $reason)
    {
        if ($this->eventDispatcher) {
            $event = new KafkaEvent($origin);
            $event->setErrorCode($errorCode);
            $event->setReason($reason);

            $this
                ->eventDispatcher
                ->dispatch(KafkaEvent::EVENT_ERROR_NAME, $event);
        }
    }

    /**
     * @param string $key
     * @return null|KafkaEvent
     */
    protected function getEvent(string $key)
    {
        $event = $this->events[$key] ?? null;
        if (!is_null($event)) {
            unset($this->events[$key]);

            return $event;
        }
    }
}
