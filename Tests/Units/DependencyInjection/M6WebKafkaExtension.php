<?php

namespace M6Web\Bundle\KafkaBundle\Tests\Units\DependencyInjection;

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
        $consumerDefinition = $this->getMockDefinition();
        $container = $this->getContainerForConfiguration('config', $consumerDefinition);
        $container->compile();

        $this
            ->boolean($container->has('m6_web_kafka.consumer.consumer1'))
                ->isTrue()
            ->mock($container)
                ->call('setDefinition')
                    ->withArguments('m6_web_kafka.consumer.consumer1', $consumerDefinition)
                        ->atLeastOnce()
        ;
    }

    /**
     * @return void
     */
    public function testShouldGetACorrectConfigurationForProducer()
    {
        $producerDefinition = $this->getMockDefinition();
        $container = $this->getContainerForConfiguration('config', $producerDefinition);
        $container->compile();

        $this
            ->boolean($container->has('m6_web_kafka.producer.producer1'))
                ->isTrue()
            ->mock($container)
                ->call('setDefinition')
                    ->withArguments('m6_web_kafka.producer.producer1', $producerDefinition)
                        ->atLeastOnce()
        ;
    }

    /**
     * @param string                                                 $fixtureName
     * @param \mock\Symfony\Component\DependencyInjection\Definition $definition
     *
     * @return \mock\Symfony\Component\DependencyInjection\ContainerBuilder
     */
    protected function getContainerForConfiguration(string $fixtureName, $definition): \mock\Symfony\Component\DependencyInjection\ContainerBuilder
    {
        $extension = new \mock\M6Web\Bundle\KafkaBundle\DependencyInjection\M6WebKafkaExtension();
        $extension->getMockController()->getDefinition = $definition;
        $extension->getMockController()->getRdKafkaConf = $this->getRdKafkaConfMock();
        $extension->getMockController()->getTopicConf = $this->getTopicConfMock();

        $container = new \mock\Symfony\Component\DependencyInjection\ContainerBuilder();
        $container->set('event_dispatcher', $this->getEventDispatcherMock());
        $container->registerExtension($extension);

        $loader = new YamlFileLoader($container, new FileLocator(__DIR__.'/../../../Tests/Fixtures/'));
        $loader->load($fixtureName.'.yml');

        return $container;
    }

    /**
     * @return \mock\Symfony\Component\DependencyInjection\Definition
     */
    protected function getMockDefinition(): \mock\Symfony\Component\DependencyInjection\Definition
    {
        $definition = new \mock\Symfony\Component\DependencyInjection\Definition();
        $definition->getMockController()->addMethodCall = $definition;
        $definition->getMockController()->getClass = 'M6Web\Bundle\KafkaBundle\FixturesRdKafkaConsumerManagerStub' ;

        return $definition;
    }

    /**
     * @return \mock\RdKafka\Conf
     */
    protected function getRdKafkaConfMock(): \mock\RdKafka\Conf
    {
        $mock = new \mock\RdKafka\Conf();
        $mock->set('group.id', 'myConsumerGroup');
        $mock->getMockController()->set = true;

        return $mock;
    }

    /**
     * @return \mock\RdKafka\Conf
     */
    protected function getTopicConfMock(): \mock\RdKafka\TopicConf
    {
        $mock = new  \mock\RdKafka\TopicConf();
        $mock->getMockController()->set = true;

        return $mock;
    }
}
