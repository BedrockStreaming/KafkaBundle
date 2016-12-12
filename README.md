# KafkaBundle

Configuration and use of KafkaBundle are based on the [RdKafka extension](https://arnaud-lb.github.io/php-rdkafka/phpdoc/book.rdkafka.html). 
[Kafka documentation](http://kafka.apache.org/documentation.html)

##Installation

###For Symfony ###

````
{
    "require": {
        "m6web/kafka-bundle": "~0.1",
    }
}
````

Register the bundle:

```php
// app/AppKernel.php

public function registerBundles()
{
    $bundles = array(
        new M6Web\Bundle\KafkaBundle\M6WebKafkaBundle(),
    );
}
```

Install the bundle:

```
$ composer update m6web/kafka-bundle
```

## Usage ##

Add the `m6_web_kafka` section in your configuration file.

By default, the sf3 event dispatcher will throw an event on each command. To disable this feature: 

```yaml
m6_web_kafka:
   event_dispatcher: false
```   

Here a configuration example: 

```yaml
m6_web_kafka:
    event_dispatcher: false
    producers:
       producer1:
           brokers:
               - "127.0.0.1"
               - "10.05.05.19"
           log_level: 3
           topics:
               test1000:
                   conf:
                       auto.commit.interval.ms: "1000"
                   strategy_partition: "2"
               test1005:
                   conf:
                       auto.commit.interval.ms: "1000"
                   strategy_partition: "2"

    consumers:
        consumer1:
            brokers:
                 - 127.0.0.1
                 - 10.05.05.19
            log_level: 3
            topics:
                test1000:
                   conf:
                       auto.commit.interval.ms: "1000"
                test1001:
                    conf:
                        auto.commit.interval.ms: "1000"
```

Note that you can configure your worker to work as a high level by setting the "group.id" option in the configuration:
 ```yaml
 conf:
     metadata.broker.list: '127.0.0.1'
     group.id: 'myConsumerGroup'
 ```
###Producer

A producer will be used to send messages to the server.

In the Kafka Model, messages are sent to topics partitioned and stored on brokers.
This means that in the configuration for a producer you will have to specify the brokers, topics and optionnaly the log level and the strategy partitioner.

Because of RdKafka extension limitations, you cannot configure the partitions number or replication factor from the bundle. 
You must do that from the command line.

After setting your producers with their options, you will be able to produce a message using the `produce` method :

```php
$producer->produce('message', RD_KAFKA_PARTITION_UA, '12345');
```

- The first argument is the message to send.
- The second argument is the partition where to produce the message. 
By default, the value is `RD_KAFKA_PARTITION_UA` which means that the message will be sent to a random partition.
- The third argument is a key if the strategy partitioner is by key.

The `RD_KAFKA_PARTITION_UA` constant is used according the strategy partitioner.
- If the strategy partitioner is `random` (`RD_KAFKA_MSG_PARTITIONER_RANDOM`), messages are assigned to partitions randomly.
- If the stratefy partitioner is `consistent` (`RD_KAFKA_MSG_PARTITIONER_CONSISTENT`) with a key defined, messages are assigned to the partition whose the id maps the hash of the key.
If there is no key defined, messages are assigned to the same partition.

###Consumer

A consumer will be used to get a message from differents topics.
You can choose to set only one topic by consumer.

In the Kafka Model, messages are consumed from topics partitioned and stored on brokers.
This means that for a consumer you will have to specify the brokers and topics in the configuration.

To consume messages, first, you will have to use the `consumeStart` method:
```php
$consumer->consumeStart();
```

And then the `consume` method to consume a message:
```php
$consumer->consume();
```

To know where you stop to consume messages, you can use the `getTopicsConsumptionState`

```php
$consumer->getTopicsConsumptionState();
```

It will give you an object `TopicsConsumptionState`.

To start again consuming messages where you stop, you can use the `setTopicsConsumptionState` method before using the `consumeStart` method.

```php
$topicsState = $consumer->getTopicsConsumptionState();
$consumer->setTopicsConsumptionState($topicsState);
$consumer->consumeStart();
$consumer->consume();
```

###TopicsConsumptionState

####You can modify the `TopicsConsumptionState` object.

The `defineTopic(string $topicName, bool $defaultValuesFromServer = false)` method can be used to define a topic.
- The first argument is the topic name to consume.
- If the second argument is set to true, all the partitions of the topic will be consumed from the beginning.

The `defineAPartitionForATopic(string $topicName, int $partitionId, int $offset = RD_KAFKA_OFFSET_BEGINNING`) method can be used to define a partition for a defined topic.
- The first argument is the topic name to define the partition to consume
- The second argument is the id of the partitition to consume
- The third argument is the last offset considered as already read

The `defineAPartitionForAUniqueTopicSet(int $partitionId, int $offset = RD_KAFKA_OFFSET_BEGINNING)` method can be used to define a partition without specifying the topic if there is only one topic
- The first argument is the id of the partitition to consume
- The second argument is the offset where you want to start consuming

####You can get information from the `TopicsConsumptionState` object.

The `getTopics: array` method can be used to get the topics list to consume.

The `getPartitionsForATopic(string $topicName): array` method can be used to get the partitions list to consume for a specific topic.

The `getOffsetForAPartitionForATopic(string $topicName, int $partitionId): int` method can be used to get the last offset read for a specific partition for a specific topic.

The `getOffsetForAPartitionForAUniqueTopicSet(int $partitionId): int` method can be used to get the last offset read for a specific partition if you have only one topic set.

The `getPartitionsForAUniqueTopicSet(): array` method can be used to get the partitions list to consume if you have only one topic set.

###Exceptions list
- BadUsageOfConsumerWithSeveralTopicsException
- EntityNotSetException
- LogLevelNotSetException
- NoBrokerSetException
- NoConsumerStartLaunchException
- NoTopicsConsumptionStateSetException
- PartitionForTopicNotFoundException
- PartitionNotFoundException
- TopicNotFoundException
- TopicNotSetException
