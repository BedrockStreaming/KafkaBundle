<?php
namespace M6Web\Bundle\KafkaBundle\Exceptions;

/**
 * Class PartitionForTopicNotFoundException
 * @package M6Web\Bundle\KafkaBundle\Exceptions
 *
 * A class to handle exceptions when trying to use a partition (linked to a topic) that does not exist
 */
class PartitionForTopicNotFoundException extends \Exception
{
}
