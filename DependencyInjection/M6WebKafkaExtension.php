<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\DependencyInjection;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\DependencyInjection\Extension\Extension;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;
use Symfony\Component\DependencyInjection\Definition;

/**
 * Class M6WebKafkaExtension
 * @package M6Web\Bundle\KafkaBundle\DependencyInjection
 *
 * This is the class that loads and manages your bundle configuration
 */
class M6WebKafkaExtension extends Extension
{
    /**
     * {@inheritDoc}
     */
    public function load(array $configs, ContainerBuilder $container)
    {
        $configuration = new Configuration();
        $config = $this->processConfiguration($configuration, $configs);

        $loader = new YamlFileLoader($container, new FileLocator(__DIR__.'/../Resources/config'));
        $loader->load('services.yml');

        $this->loadProducers($container, $config);
        $this->loadConsumers($container, $config);
    }

    /**
     * @param string $className
     * @return Definition
     */
    public function getDefinition(string $className): Definition
    {
        return new Definition($className);
    }

    /**
     * @return \RdKafka\Conf
     */
    public function getRdKafkaConf(): \RdKafka\Conf
    {
        return new \RdKafka\Conf();
    }

    /**
     * @return \RdKafka\TopicConf
     */
    public function getTopicConf(): \RdKafka\TopicConf
    {
        return new \RdKafka\TopicConf();
    }

    /**
     * @param ContainerBuilder $container
     * @param array            $config
     */
    protected function loadProducers(ContainerBuilder $container, array $config)
    {
        foreach ($config['producers'] as $key => $producer) {
            $producerDefinition = $this->getDefinition('M6Web\Bundle\KafkaBundle\RdKafkaProducerManager');

            $this->setEventDispatcher($config, $producerDefinition);

            $conf = $this->getReadyRdKafkaConf($producer['conf']);
            $rdKafkaProducer = new \RdKafka\Producer($conf);

            $producerDefinition->addMethodCall('setRdKafkaProducer', [$rdKafkaProducer]);
            $this->setProducerManager($producer, $producerDefinition);

            $container->setDefinition(
                sprintf('m6_web_kafka.producer.%s', $key),
                $producerDefinition
            );
        }
    }

    /**
     * @param ContainerBuilder $container
     * @param array $config
     */
    protected function loadConsumers(ContainerBuilder $container, array $config)
    {
        foreach ($config['consumers'] as $key => $consumer) {
            $consumerDefinition = $this->getDefinition('M6Web\Bundle\KafkaBundle\RdKafkaConsumerManager');

            $this->setEventDispatcher($config, $consumerDefinition);

            $topicConf = $this->getReadyTopicConf($consumer['topicConf']);
            $conf = $this->getReadyRdKafkaConf($consumer['conf']);
            $conf->setDefaultTopicConf($topicConf);

            $kafkaConsumer = new \RdKafka\KafkaConsumer($conf);

            $consumerDefinition->addMethodCall('setRdKafkaKafkaConsumer', [$kafkaConsumer]);
            $consumerDefinition->addMethodCall('addTopic', [$consumer['topics'], $kafkaConsumer]);
            $consumerDefinition->addMethodCall('setTimeoutConsumingQueue', [$consumer['timeout_consuming_queue']]);

            $container->setDefinition(
                sprintf('m6_web_kafka.consumer.%s', $key),
                $consumerDefinition
            );
        }
    }

    /**
     * @param array $config
     * @param $entityDefinition
     */
    protected function setEventDispatcher(array $config, $entityDefinition)
    {
        if ($config['event_dispatcher'] === true) {
            $entityDefinition->addMethodCall(
                'setEventDispatcher',
                [
                    new Reference('event_dispatcher'),
                ]
            );
        }
    }

    /**
     * @param array      $entity
     * @param Definition $entityDefinition
     */
    protected function setProducerManager(array $entity, Definition $entityDefinition)
    {
        $brokers = implode(',', $entity['brokers']);

        $entityDefinition
            ->addMethodCall('addBrokers', [$brokers]);

        $entityDefinition
            ->addMethodCall('setLogLevel', [$entity['log_level']]);

        foreach ($entity['topics'] as $topicName => $topic) {
            $topicConf = $this->getReadyTopicConf($topic['conf']);
            $topicConf->setPartitioner((int) $topic['strategy_partition']);
            $entityDefinition->addMethodCall('addTopic', [$topicName, $topicConf]);
        }
    }

    /**
     * @param array $confToSet
     * @return \RdKafka\TopicConf
     */
    protected function getReadyTopicConf(array $confToSet = []): \RdKafka\TopicConf
    {
        $topicConf = $this->getTopicConf();

        array_walk($confToSet, function ($confValue, $confIndex) use ($topicConf) {
            $topicConf->set($confIndex, (string) $confValue);
        });

        return $topicConf;
    }

    /**
     * @param \RdKafka\Conf $rdKafkaConf
     * @param array         $confData
     *
     * @return \RdKafka\Conf
     */
    protected function getReadyRdKafkaConf(array $confToSet): \RdKafka\Conf
    {
        $rdKafkaConf = $this->getRdKafkaConf();

        array_walk($confToSet, function ($confValue, $confIndex) use ($rdKafkaConf) {
            $rdKafkaConf->set($confIndex, (string) $confValue);
        });

        // Set a rebalance callback to log automatically assign partitions
        $rdKafkaConf->setRebalanceCb($this->handlePartitionsAssignment());

        return $rdKafkaConf;
    }

    /**
     * @return Callable
     */
    protected function handlePartitionsAssignment(): Callable
    {
        return function (\RdKafka\KafkaConsumer $kafka, $error, array $partitions = null) {
            if ($error === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                $kafka->assign($partitions);

                return;
            }

            if ($error === RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
                $kafka->assign(null);

                return;
            }

            throw new \Exception($error);
        };
    }
}
