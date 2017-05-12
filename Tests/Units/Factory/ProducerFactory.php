<?php

namespace M6Web\Bundle\KafkaBundle\Tests\Units\Factory;

use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;

/**
 * Class ProducerFactory
 */
class ProducerFactory extends BaseUnitTest
{
    public function testGet()
    {
        $producerClass = 'RdKafka\Producer';
        $producerData  = [
            'configuration' => [],
            'brokers' => [
                '127.0.0.1'
            ],
            'log_level' => LOG_ALERT,
            'topics' => [
                'test' => [
                    'configuration' => [
                        'auto.commit.interval.ms' => '1000'
                    ],
                    'strategy_partition' => 2
                ]
            ],
            'events_poll_timeout' => -1
        ];

        $this
            ->given(
                $this->newTestedInstance(new \mock\RdKafka\Conf(), new \RdKafka\TopicConf()),
                $producerManager = $this->testedInstance->get($producerClass, $producerData)
            )
            ->then
                ->object($producerManager)
                    ->isInstanceOf('M6Web\Bundle\KafkaBundle\Manager\ProducerManager')
        ;
    }
}
