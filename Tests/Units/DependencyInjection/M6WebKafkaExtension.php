<?php

namespace M6Web\Bundle\KafkaBundle\Tests\Units\DependencyInjection;

use M6Web\Bundle\KafkaBundle\DependencyInjection\M6WebKafkaExtension as TestedClass;
use M6Web\Bundle\KafkaBundle\Manager\ConsumerManager;
use M6Web\Bundle\KafkaBundle\Manager\ProducerManager;
use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;

/**
 * Class M6WebKafkaExtension
 * @package M6Web\Bundle\KafkaBundle\Tests\Units\DependencyInjection
 *
 * A class to test configuration loading
 */
class M6WebKafkaExtension extends BaseUnitTest
{
    /**
     * @return void
     */
    public function testShouldGetACorrectConfigurationForConsumer()
    {
        $container = $this->getContainerForConfiguration('config');
        $container->compile();

        $this
            ->boolean($container->has('m6_web_kafka.consumer.consumer1'))
                ->isTrue()
            ->object($container->get('m6_web_kafka.consumer.consumer1'))
                ->isInstanceOf('M6Web\Bundle\KafkaBundle\Manager\ConsumerManager')
        ;
    }

    /**
     * @return void
     */
    public function testShouldGetACorrectConfigurationForProducer()
    {
        $container = $this->getContainerForConfiguration('config');
        $container->compile();

        $this
            ->boolean($container->has('m6_web_kafka.producer.producer1'))
                ->isTrue()
            ->object($container->get('m6_web_kafka.producer.producer1'))
                ->isInstanceOf('M6Web\Bundle\KafkaBundle\Manager\ProducerManager')
        ;
    }

    /**
     * @param string                                                 $fixtureName
     * @param \mock\Symfony\Component\DependencyInjection\Definition $definition
     *
     * @return \mock\Symfony\Component\DependencyInjection\ContainerBuilder
     */
    protected function getContainerForConfiguration(string $fixtureName): \mock\Symfony\Component\DependencyInjection\ContainerBuilder
    {
        $extension = new TestedClass();

        $container = new \mock\Symfony\Component\DependencyInjection\ContainerBuilder();
        $container->set('event_dispatcher', $this->getEventDispatcherMock());
        $container->set('m6web_kafka.producer_factory', $this->getProducerFactoryMock());
        $container->set('m6web_kafka.consumer_factory', $this->getConsumerFactoryMock());
        $container->registerExtension($extension);

        $loader = new YamlFileLoader($container, new FileLocator(__DIR__.'/../../../Tests/Fixtures/'));
        $loader->load($fixtureName.'.yml');

        return $container;
    }

    /**
     * @return \mock\M6Web\Bundle\KafkaBundle\Factory\ProducerFactory
     */
    protected function getProducerFactoryMock()
    {
        $this->mockGenerator->orphanize('__construct');

        $mock = new \mock\M6Web\Bundle\KafkaBundle\Factory\ProducerFactory();
        $mock->getMockController()->get = new ProducerManager();

        return $mock;
    }

    /**
     * @return \mock\M6Web\Bundle\KafkaBundle\Factory\ConsumerFactory
     */
    protected function getConsumerFactoryMock()
    {
        $this->mockGenerator->orphanize('__construct');

        $mock = new \mock\M6Web\Bundle\KafkaBundle\Factory\ConsumerFactory();
        $mock->getMockController()->get = new ConsumerManager();

        return $mock;
    }
}
