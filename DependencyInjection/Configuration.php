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
                ->scalarNode('prefix_services_name')->defaultValue('m6_web_kafka')->end()
                ->booleanNode('event_dispatcher')->defaultTrue()->end()
                ->arrayNode('consumers')
                    ->useAttributeAsKey('key')
                    ->prototype('array')
                        ->children()
                            ->arrayNode('configuration')
                                ->prototype('scalar')->end()
                                ->defaultValue([])
                                ->normalizeKeys(false)
                            ->end()
                            ->arrayNode('topicConfiguration')
                                ->prototype('scalar')->end()
                                ->defaultValue([])
                                ->normalizeKeys(false)
                            ->end()
                            ->integerNode('timeout_consuming_queue')->defaultValue(1000)->end()
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
                            ->arrayNode('configuration')
                                ->prototype('scalar')->end()
                                ->defaultValue([])
                                ->normalizeKeys(false)
                            ->end()
                            ->arrayNode('brokers')
                                ->prototype('scalar')->end()
                            ->end()
                            ->integerNode('log_level')->defaultValue(LOG_WARNING)->end()
                            ->integerNode('events_poll_timeout')->defaultValue(500)->end()
                            ->arrayNode('topics')
                                ->useAttributeAsKey('key')
                                ->prototype('array')
                                    ->children()
                                        ->arrayNode('configuration')
                                            ->prototype('scalar')->end()
                                            ->defaultValue([])
                                            ->normalizeKeys(false)
                                        ->end()
                                        ->integerNode('strategy_partition')->end()
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
