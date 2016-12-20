<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\DependencyInjection;

use M6Web\Bundle\KafkaBundle\Helper\PartitionAssignment;
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
            $producerDefinition = $this->getDefinition('M6Web\Bundle\KafkaBundle\Manager\RdKafkaProducerManager');

            $this->setEventDispatcher($config, $producerDefinition);

            $conf = $this->getReadyRdKafkaConf($producer['conf']);
            $rdKafkaProducer = new \RdKafka\Producer($conf);

            $producerDefinition->addMethodCall('setProducer', [$rdKafkaProducer]);

            $producerDefinition
                ->addMethodCall('addBrokers', [implode(',', $producer['brokers'])])
                ->addMethodCall('setLogLevel', [$producer['log_level']]);

            foreach ($producer['topics'] as $topicName => $topic) {
                $topicConf = $this->getReadyTopicConf($topic['conf']);
                $topicConf->setPartitioner((int) $topic['strategy_partition']);
                $producerDefinition->addMethodCall('addTopic', [$topicName, $topicConf]);
            }

            $container->setDefinition(
                sprintf('m6_web_kafka.producer.%s', $key),
                $producerDefinition
            );
        }
    }

    /**
     * @param ContainerBuilder $container
     * @param array            $config
     */
    protected function loadConsumers(ContainerBuilder $container, array $config)
    {
        foreach ($config['consumers'] as $key => $consumer) {
            $consumerManager = $this->getDefinition('M6Web\Bundle\KafkaBundle\Manager\RdKafkaConsumerManager');

            $this->setEventDispatcher($config, $consumerManager);

            $topicConf = $this->getReadyTopicConf($consumer['topicConf']);
            $conf = $this->getReadyRdKafkaConf($consumer['conf']);
            $conf->setDefaultTopicConf($topicConf);

            $kafkaConsumer = new \RdKafka\KafkaConsumer($conf);

            $consumerManager->addMethodCall('setConsumer', [$kafkaConsumer]);
            $consumerManager->addMethodCall('addTopic', [$consumer['topics'], $kafkaConsumer]);
            $consumerManager->addMethodCall('setTimeoutConsumingQueue', [$consumer['timeout_consuming_queue']]);

            $container->setDefinition(
                sprintf('m6_web_kafka.consumer.%s', $key),
                $consumerManager
            );
        }
    }

    /**
     * @param array      $config
     * @param Definition $definition
     */
    protected function setEventDispatcher(array $config, Definition $definition)
    {
        if ($config['event_dispatcher'] === true) {
            $definition->addMethodCall(
                'setEventDispatcher',
                [
                    new Reference('event_dispatcher'),
                ]
            );
        }
    }

    /**
     * @param array $confToSet
     * @return \RdKafka\TopicConf
     */
    protected function getReadyTopicConf(array $confToSet = []): \RdKafka\TopicConf
    {
        $topicConf = $this->getTopicConf();

        $revertConfToSet = array_flip($confToSet);
        array_walk($revertConfToSet, [$topicConf, 'set']);

        return $topicConf;
    }

    /**
     * @param \RdKafka\Conf $rdKafkaConf
     *
     * @return \RdKafka\Conf
     */
    protected function getReadyRdKafkaConf(array $confToSet): \RdKafka\Conf
    {
        $rdKafkaConf = $this->getRdKafkaConf();

        $revertConfToSet = array_flip($confToSet);
        array_walk($revertConfToSet, [$rdKafkaConf, 'set']);

        // Set a rebalance callback to log automatically assign partitions
        $rdKafkaConf->setRebalanceCb(PartitionAssignment::handlePartitionsAssignment());

        return $rdKafkaConf;
    }
}
