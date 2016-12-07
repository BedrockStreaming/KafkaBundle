<?php
declare(strict_types = 1);

namespace M6Web\Bundle\KafkaBundle\DependencyInjection;

use M6Web\Bundle\KafkaBundle\Consumer\TopicsConsumptionState;
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
     * @param ContainerBuilder $container
     * @param Extension        $entity
     * @return object
     */
    public function getConf(ContainerBuilder $container, $entity)
    {
        return $container->get('m6_web_kafka.conf')->getConf(new \RdKafka\Conf(), $entity['conf']);
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
     * @param ContainerBuilder $container
     * @param array            $config
     */
    protected function loadProducers(ContainerBuilder $container, array $config)
    {
        foreach ($config['producers'] as $key => $producer) {
            $producerDefinition = $this->getDefinition('M6Web\Bundle\KafkaBundle\Producer\RdKafkaProducerManager');

            $this->setEventDispatcher($config, $producerDefinition);

            $producerDefinition
                ->addMethodCall('setEntity', [new \RdKafka\Producer($this->getConf($container, $producer))]);
            $this->setEntityManager($producer, $producerDefinition);

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
            $consumerDefinition = $this->getDefinition('M6Web\Bundle\KafkaBundle\Consumer\RdKafkaConsumerManager');

            $this->setEventDispatcher($config, $consumerDefinition);

            $consumerDefinition->addMethodCall('setEntity', [new \RdKafka\Consumer($this->getConf($container, $consumer))]);
            $consumerDefinition->addMethodCall('setTimeoutConsumingQueue', [$consumer['timeout_consuming_queue']]);
            $this->setEntityManager($consumer, $consumerDefinition);
            $consumerDefinition->addMethodCall('defineTopicsConsumptionState', [new TopicsConsumptionState()]);


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
    protected function setEntityManager(array $entity, Definition $entityDefinition)
    {
        $brokers = implode(',', $entity['brokers']);

        $entityDefinition
            ->addMethodCall('addBrokers', [$brokers]);

        $entityDefinition
            ->addMethodCall('setLogLevel', [$entity['log_level']]);

        foreach ($entity['topics'] as $topicName => $topic) {
            $entityDefinition->addMethodCall('addTopic', [$topicName, new \RdKafka\TopicConf(), $topic['conf']]);
        }
    }
}
