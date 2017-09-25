<?php

namespace M6Web\Bundle\KafkaBundle\Handler;

abstract class MessageHandlerAbstract implements MessageHandlerInterface
{

    public function endOfPartitionReached()
    {
        return null;
    }

}
