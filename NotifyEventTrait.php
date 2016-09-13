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
        if ($this->eventDispatcher) {
            $this
                ->eventDispatcher
                ->dispatch('kafka.event', new EventLog($origin));
        }
    }
}
