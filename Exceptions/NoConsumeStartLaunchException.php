<?php
namespace M6Web\Bundle\KafkaBundle\Exceptions;

/**
 * Class NoConsumeStartLaunchException
 * @package M6Web\Bundle\KafkaBundle\Exceptions
 *
 * A class to handle exceptions when trying to consume messages without having initialized the consuming action by launching the "consume start" action first
 */
class NoConsumeStartLaunchException extends \Exception
{
    protected $message = 'You should launch the "consume start" action';
}
