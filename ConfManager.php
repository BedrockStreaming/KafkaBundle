<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle;

/**
 * Class Conf
 * @package M6Web\Bundle\KafkaBundle
 *
 * A class to configure \RdKafka\Consumer or \RdKafka\Producer from yml configuration
 */
class ConfManager
{
    /**
     * @param \RdKafka\Conf $rdKafkaConf
     * @param array         $confData
     * @return \RdKafka\Conf
     */
    public function getConf(\RdKafka\Conf $rdKafkaConf, array $confData)
    {
        foreach ($confData as $conf => $value) {
            $rdKafkaConf->set($conf, (string) $value);
        }

        return $rdKafkaConf;
    }
}
