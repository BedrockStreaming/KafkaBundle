<?php

namespace M6Web\Bundle\KafkaBundle\Handler;

use RdKafka\Message;

interface MessageHandlerInterface
{
    /**
     * Process message from kafka
     *
     * @param Message $message
     * @return mixed
     */
    public function process(Message $message);

}