<?php
namespace M6Web\Bundle\KafkaBundle\Exceptions;

/**
 * Class LogLevelNotSetException
 * @package M6Web\Bundle\KafkaBundle\Exceptions
 *
 * A class to handle exceptions when trying to use a constructed entity without having set the log level
 */
class LogLevelNotSetException extends \Exception
{
    protected $message = 'Log level not set';
}
