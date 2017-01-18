<?php

namespace M6Web\Bundle\KafkaBundle\Tests\Units\Factory;

use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;

/**
 * Class ConsumerFactory
 */
class ConsumerFactory extends BaseUnitTest
{
    public function testGet()
    {
        $consumerClass = 'RdKafka\KafkaConsumer';
        $consumerData  = [
            'configuration' => [
                'group.id' => 'myConsumerGroup'
            ],
            'topicConfiguration' => [],
            'timeout_consuming_queue' => 1,
            'topics' => [
                'test'
            ]
        ];

        $this
            ->given(
                $this->newTestedInstance(new \mock\RdKafka\Conf(), new \mock\RdKafka\TopicConf()),
                $producerManager = $this->testedInstance->get($consumerClass, $consumerData)
            )
            ->then
                ->object($producerManager)
                    ->isInstanceOf('M6Web\Bundle\KafkaBundle\Manager\ConsumerManager')
        ;
    }
}
