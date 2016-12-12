<?php
namespace M6Web\Bundle\KafkaBundle\Exceptions;

/**
 * Class NoTopicsConsumptionStateSetException
 * @package M6Web\Bundle\KafkaBundle\Exceptions
 *
 * A class to handle exceptions when trying to consume messages without topics consumption state set
 */
class NoTopicsConsumptionStateSetException extends \Exception
{
    protected $message = 'No topics consumption state set';
}
