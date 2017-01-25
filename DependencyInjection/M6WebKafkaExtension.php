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
     * @param ContainerBuilder $container
     * @param array            $config
     */
    protected function loadProducers(ContainerBuilder $container, array $config)
    {
        foreach ($config['producers'] as $key => $producerData) {
            // Create the producer with the factory
            $producerDefinition = new Definition(
                'M6Web\Bundle\KafkaBundle\Manager\ProducerManager',
                [
                    'RdKafka\Producer',
                    $producerData,
                ]
            );

            // Use a factory to build the producer
            $producerDefinition->setFactory([
                new Reference('m6web_kafka.producer_factory'),
                'get',
            ]);

            $this->setEventDispatcher($config, $producerDefinition);

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
            $consumerDefinition = new Definition(
                'M6Web\Bundle\KafkaBundle\Manager\ConsumerManager',
                [
                    'RdKafka\KafkaConsumer',
                    $consumerData,
                ]
            );

            $consumerDefinition->setFactory([
                new Reference('m6web_kafka.consumer_factory'),
                'get',
            ]);

            $this->setEventDispatcher($config, $consumerDefinition);

            $container->setDefinition(
                sprintf('m6_web_kafka.consumer.%s', $key),
                $consumerDefinition
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
}
