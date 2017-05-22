<?php
namespace M6Web\Bundle\KafkaBundle\Event;

use Symfony\Component\EventDispatcher\Event as SymfonyEvent;

/**
 * Class KafkaEvent
 * @package M6Web\Bundle\KafkaBundle\Event
 *
 * A class to send an event to say that messages were produced or consumed for monitoring
 */
class KafkaEvent extends SymfonyEvent
{
    const EVENT_NAME = 'm6web.kafka';
    const EVENT_ERROR_NAME = 'm6web.kafka.error';

    /**
     * @var string
     */
    protected $origin;

    /**
     * Command start time
     *
     * @var float
     */
    protected $executionStart;

    /**
     * Command execution time
     *
     * @var float
     */
    protected $executionTime;

    /**
     * @var int
     */
    protected $errorCode;

    /**
     * @var string
     */
    protected $reason;

    /**
     * Event constructor.
     * @param string $origin
     */
    public function __construct(string $origin)
    {
        $this->origin = $origin;
        $this->executionStart = 0;
    }

    /**
     * @return string
     */
    public function getOrigin(): string
    {
        return $this->origin;
    }

    /**
     * Set error code
     *
     * @param int $errorCode
     *
     * @return KafkaEvent
     */
    public function setErrorCode(int $errorCode): self
    {
        $this->errorCode = $errorCode;

        return $this;
    }

    /**
     * Return error code
     *
     * @return int|null
     */
    public function getErrorCode()
    {
        return $this->errorCode;
    }

    /**
     * @param string $reason
     *
     * @return KafkaEvent
     */
    public function setReason(string $reason): self
    {
        $this->reason = $reason;

        return $this;
    }

    /**
     * Return reason
     *
     * @return string|null
     */
    public function getReason()
    {
        return $this->reason;
    }

    /**
     * @return float
     */
    public function getExecutionStart(): float
    {
        return $this->executionStart;
    }

    /**
     * Set execution start of a request
     *
     * @return KafkaEvent
     */
    public function setExecutionStart(): self
    {
        $this->executionStart = microtime(true);

        return $this;
    }

    /**
     * @return KafkaEvent
     */
    public function setExecutionStop(): self
    {
        $this->executionTime = microtime(true) - $this->executionStart;

        return $this;
    }

    /**
     * @return float
     */
    public function getExecutionTime(): float
    {
        return $this->executionTime;
    }

    /**
     * Return execution time in milliseconds
     *
     * @return float
     */
    public function getTiming(): float
    {
        return $this->getExecutionTime() * 1000;
    }
}
