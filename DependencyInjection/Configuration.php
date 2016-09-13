<?php

namespace M6Web\Bundle\KafkaBundle\DependencyInjection;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

/**
 * This is the class that validates and merges configuration from your app/config files
 */
class Configuration implements ConfigurationInterface
{
    /**
     * {@inheritDoc}
     */
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('m6_web_kafka', 'array');

        $rootNode
            ->children()
                ->booleanNode('event_dispatcher')->defaultTrue()->end()
                ->arrayNode('consumers')
                    ->useAttributeAsKey('key')
                    ->prototype('array')
                        ->children()
                            ->arrayNode('conf')
                                ->prototype('scalar')->end()
                                    ->defaultValue(array())
                                    ->normalizeKeys(false)
                            ->end()
                            ->arrayNode('topicConf')
                                ->prototype('scalar')->end()
                                    ->defaultValue(array())
                                    ->normalizeKeys(false)
                            ->end()
                            ->scalarNode('timeout_consuming_queue')->defaultValue(1000)->end()
                            ->arrayNode('topics')
                                ->prototype('scalar')->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('producers')
                    ->useAttributeAsKey('key')
                    ->prototype('array')
                        ->children()
                            ->arrayNode('conf')
                                ->prototype('scalar')->end()
                                    ->defaultValue(array())
                                    ->normalizeKeys(false)
                            ->end()
                            ->arrayNode('brokers')
                                ->prototype('scalar')->end()
                            ->end()
                            ->scalarNode('log_level')->defaultValue(LOG_WARNING)->end()
                            ->arrayNode('topics')
                                ->useAttributeAsKey('key')
                                ->prototype('array')
                                    ->children()
                                        ->arrayNode('conf')
                                            ->prototype('scalar')->end()
                                            ->defaultValue(array())
                                            ->normalizeKeys(false)
                                        ->end()
                                        ->scalarNode('strategy_partition')->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end();

        return $treeBuilder;
    }
}
