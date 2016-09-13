<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\Fixtures;

use M6Web\Bundle\KafkaBundle\ConfManager;

/**
 * Class ConfStub
 * @package M6Web\Bundle\KafkaBundle\Fixtures
 *
 * A class to stub the Conf class to test dependency injection
 */
class ConfManagerStub extends ConfManager
{
    /**
     * @param \RdKafka\Conf $rdKafkaConf
     * @param array         $confData
     * @return \RdKafka\Conf
     */
    public function getConf(\RdKafka\Conf $rdKafkaConf, array $confData)
    {
        return $rdKafkaConf;
    }
}
