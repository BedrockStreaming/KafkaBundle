<?php

namespace M6Web\Bundle\KafkaBundle\Tests\Units\DependencyInjection;

use M6Web\Bundle\KafkaBundle\DependencyInjection\M6WebKafkaExtension as Base;
use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;
use Symfony\Component\DependencyInjection\ParameterBag\ParameterBag;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;
use M6Web\Bundle\KafkaBundle\Producer\RdKafkaProducerManager;
use M6Web\Bundle\KafkaBundle\Consumer\RdKafkaConsumerManager;

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
    public function testShouldGetACorrectConfigurationForProducer()
    {
        $container = $this->getContainerForConfiguration('config');
        $container->compile();

        $this
            ->boolean($container->has('m6_web_kafka.producer.producer1'))
                ->isTrue()
            ->object($producer = $container->get('m6_web_kafka.producer.producer1'))
                ->isInstanceOf(RdKafkaProducerManager::class)
        ;
    }

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
            ->object($consumer = $container->get('m6_web_kafka.consumer.consumer1'))
                ->isInstanceOf(RdKafkaConsumerManager::class)
        ;
    }

    /**
     * @param $fixtureName
     *
     * @return ContainerBuilder
     */
    protected function getContainerForConfiguration($fixtureName): ContainerBuilder
    {
        $extension = new Base();
        $parameterBag = new ParameterBag(array('kernel.debug' => true));
        $container = new ContainerBuilder($parameterBag);
        $container->set('event_dispatcher', $this->getEventDispatcherMock());
        $container->registerExtension($extension);

        $loader = new YamlFileLoader($container, new FileLocator(__DIR__.'/../../../Fixtures/'));
        $loader->load($fixtureName.'.yml');

        return $container;
    }

    /**
     * @return \mock\M6Web\Bundle\KafkaBundle\Conf
     */
    protected function getConfMock(): \mock\M6Web\Bundle\KafkaBundle\ConfManager
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\M6Web\Bundle\KafkaBundle\ConfManager();
        $mock->getMockController()->getConf = true;

        return $mock;
    }
}
