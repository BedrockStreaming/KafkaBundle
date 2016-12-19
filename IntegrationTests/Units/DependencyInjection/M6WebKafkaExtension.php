<?php

namespace M6Web\Bundle\KafkaBundle\Tests\Units\DependencyInjection;

use M6Web\Bundle\KafkaBundle\DependencyInjection\M6WebKafkaExtension as Base;
use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;
use Symfony\Component\DependencyInjection\ParameterBag\ParameterBag;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;
use M6Web\Bundle\KafkaBundle\Manager\RdKafkaProducerManager;
use M6Web\Bundle\KafkaBundle\Manager\RdKafkaConsumerManager;

/**
 * Class M6WebKafkaExtension
 * @package M6Web\Bundle\KafkaBundle\Tests\Units\DependencyInjection
 *
 * A class to test real configuration loading
 */
class M6WebKafkaExtension extends BaseUnitTest
{
    /**
     * @return void
     */
    public function testShouldProduceAMessageAndConsumeItAfter()
    {
        $container = $this->getContainerForConfiguration('config');
        $container->compile();

        $this
            ->boolean($container->has('m6_web_kafka.consumer.consumer1'))
                ->isTrue()
            ->object($producer = $container->get('m6_web_kafka.producer.producer1'))
                ->isInstanceOf(RdKafkaProducerManager::class)
            ->variable($producer->produce('\O/'))
            ->object($consumer = $container->get('m6_web_kafka.consumer.consumer1'))
                ->isInstanceOf(RdKafkaConsumerManager::class)
            ->variable($message1 = $consumer->consume())
            ->variable($message1->payload)
                ->isEqualTo('\O/')
        ;
    }

    /**
     * @param $fixtureName
     *
     * @return ContainerBuilder
     */
    protected function getContainerForConfiguration(string $fixtureName): ContainerBuilder
    {
        $extension = new Base();
        $parameterBag = new ParameterBag(['kernel.debug' => true]);
        $container = new ContainerBuilder($parameterBag);
        $container->set('event_dispatcher', $this->getEventDispatcherMock());
        $container->registerExtension($extension);

        $loader = new YamlFileLoader($container, new FileLocator(__DIR__.'/../../../IntegrationTests/Fixtures/'));
        $loader->load($fixtureName.'.yml');

        return $container;
    }
}
