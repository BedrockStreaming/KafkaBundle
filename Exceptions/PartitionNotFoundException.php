<?php
namespace M6Web\Bundle\KafkaBundle\Exceptions;

/**
 * Class PartitionNotFoundException
 * @package M6Web\Bundle\KafkaBundle\Exceptions
 *
 * A class to handle exceptions when trying to use a partition that does not exist
 */
class PartitionNotFoundException extends \Exception
{
    protected $message = 'Partition does not exist';
}
