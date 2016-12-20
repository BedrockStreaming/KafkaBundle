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
    public function getConfigurationForConsumerOrProducer(): \RdKafka\Conf
    {
        return new \RdKafka\Conf();
    }

    /**
     * @return \RdKafka\TopicConf
     */
    public function getTopicConfiguration(): \RdKafka\TopicConf
    {
        return new \RdKafka\TopicConf();
    }

    /**
     * @param ContainerBuilder $container
     * @param array            $config
     */
    protected function loadProducers(ContainerBuilder $container, array $config)
    {
        foreach ($config['producers'] as $key => $producerData) {
            $producerDefinition = $this->getDefinition('M6Web\Bundle\KafkaBundle\Manager\ProducerManager');

            $this->setEventDispatcher($config, $producerDefinition);

            $configuration = $this->getReadyConfiguration($producerData['configuration']);
            $producer = new \RdKafka\Producer($configuration);

            $producerDefinition->addMethodCall('setProducer', [$producer]);

            $producerDefinition
                ->addMethodCall('addBrokers', [implode(',', $producerData['brokers'])])
                ->addMethodCall('setLogLevel', [$producerData['log_level']]);

            foreach ($producerData['topics'] as $topicName => $topic) {
                $topicConfiguration = $this->getReadyTopicConf($topic['configuration']);
                $topicConfiguration->setPartitioner((int) $topic['strategy_partition']);
                $producerDefinition->addMethodCall('addTopic', [$topicName, $topicConfiguration]);
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
        foreach ($config['consumers'] as $key => $consumerData) {
            $consumerManager = $this->getDefinition('M6Web\Bundle\KafkaBundle\Manager\ConsumerManager');

            $this->setEventDispatcher($config, $consumerManager);

            $topicConfiguration = $this->getReadyTopicConf($consumerData['topicConfiguration']);
            $configuration = $this->getReadyConfiguration($consumerData['configuration']);
            $configuration->setDefaultTopicConf($topicConfiguration);

            $consumer = new \RdKafka\KafkaConsumer($configuration);

            $consumerManager->addMethodCall('setConsumer', [$consumer]);
            $consumerManager->addMethodCall('addTopic', [$consumerData['topics'], $consumer]);
            $consumerManager->addMethodCall('setTimeoutConsumingQueue', [$consumerData['timeout_consuming_queue']]);

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
     * @param array $configurationToSet
     * @return \RdKafka\TopicConf
     */
    protected function getReadyTopicConf(array $configurationToSet = []): \RdKafka\TopicConf
    {
        $topicConfiguration = $this->getTopicConfiguration();

        $revertConfigurationToSet = array_flip($configurationToSet);
        array_walk($revertConfigurationToSet, [$topicConfiguration, 'set']);

        return $topicConfiguration;
    }

    /**
     * @param array $configurationToSet
     *
     * @return \RdKafka\Conf
     */
    protected function getReadyConfiguration(array $configurationToSet = []): \RdKafka\Conf
    {
        $configuration = $this->getConfigurationForConsumerOrProducer();

        $revertConfigurationToSet = array_flip($configurationToSet);
        array_walk($revertConfigurationToSet, [$configuration, 'set']);

        // Set a rebalance callback to log automatically assign partitions
        $configuration->setRebalanceCb(PartitionAssignment::handlePartitionsAssignment());

        return $configuration;
    }
}
