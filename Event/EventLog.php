<?php
namespace M6Web\Bundle\KafkaBundle\Event;

use Symfony\Component\EventDispatcher\Event as SymfonyEvent;

/**
 * Class EventLog
 * @package M6Web\Bundle\KafkaBundle\Event
 *
 * A class to send an event to say that messages were produced or consumed for monitoring
 */
class EventLog extends SymfonyEvent
{
    /**
     * @var string
     */
    protected $origin;

    /**
     * Event constructor.
     * @param string $origin
     */
    public function __construct(string $origin)
    {
        $this->origin = $origin;
    }

    /**
     * @return string
     */
    public function getOrigin(): string
    {
        return $this->origin;
    }
}
