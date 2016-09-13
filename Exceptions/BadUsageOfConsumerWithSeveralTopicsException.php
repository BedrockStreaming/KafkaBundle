<?php
namespace M6Web\Bundle\KafkaBundle\Exceptions;

/**
 * Class BadUsageOfConsumerWithSeveralTopicsException
 * @package M6Web\Bundle\KafkaBundle\Exceptions
 *
 * A class to handle exceptions when getting a bad usage of a consumer with several topics
 */
class BadUsageOfConsumerWithSeveralTopicsException extends \Exception
{
    protected $message = 'Consumer gets several topics. You must specify which one to use';
}
