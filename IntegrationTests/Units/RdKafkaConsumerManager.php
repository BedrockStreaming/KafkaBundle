<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Tests\Units;

use M6Web\Bundle\KafkaBundle\Event\EventLog;
use M6Web\Bundle\KafkaBundle\RdKafkaConsumerManager as Base;
use Symfony\Component\EventDispatcher\EventDispatcher;

/**
 * Class RdKafkaConsumerManager
 * @package M6Web\Bundle\KafkaBundle\Tests\Units\Consumer
 *
 * A class to test the consumer manager
 */
class RdKafkaConsumerManager extends BaseUnitTest
{
    /**
     * @return void
     */
    public function testShouldConsumeMessages()
    {
        $topicConf = new \RdKafka\TopicConf();
        $topicConf->set('auto.offset.reset', 'smallest');
        $conf = new \RdKafka\Conf();
        $conf->set('group.id', 'myConsumerGroup');

        $conf->set('metadata.broker.list', '127.0.0.1');
        $conf->setDefaultTopicConf($topicConf);
        $conf->set('enable.auto.commit', '0');


        $kafkaConsumer = new \RdKafka\KafkaConsumer($conf);

        $this
            ->given(
                $consumer = new Base(),
                $consumer->setRdKafkaKafkaConsumer($kafkaConsumer),
                $consumer->setTimeoutConsumingQueue(120*1000),
                $consumer->addTopic(['batman'])
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                    ->isInstanceOf('\RdKafka\Message')
                    ->integer(count($result))
                        ->isEqualTo(1)
                    ->variable($result->payload)
                        ->isEqualTo("\O/")
            ;
    }
}
