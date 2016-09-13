<?php
namespace M6Web\Bundle\KafkaBundle\Exceptions;

/**
 * Class EntityNotSetException
 * @package M6Web\Bundle\KafkaBundle\Exceptions
 *
 * A class to handle exceptions when trying to use an entity not set
 */
class EntityNotSetException extends \Exception
{
    protected $message = 'Entity not set';
}
