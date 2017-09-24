# KafkaBundle

Configuration and use of KafkaBundle are based on the [RdKafka extension](https://arnaud-lb.github.io/php-rdkafka/phpdoc/book.rdkafka.html). 
To consume messages, we decided to use the high level consumer.

[Kafka documentation](http://kafka.apache.org/documentation.html)

## Installation

### For Symfony ###

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

[Librdkafka global configuration properties](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

```yaml
m6_web_kafka:
    services_name_prefix: 'custom_prefix'
    event_dispatcher: true
    producers:
       producer1:
           configuration:
               timeout.ms: 1000
               queue.buffering.max.ms: 0 # Maximum time, in milliseconds, for buffering data on the producer queue. 1000ms by default. 
           brokers:
               - '127.0.0.1'
               - '10.05.05.19'
           log_level: 3
           events_poll_timeout: 2000 #ms
           topics:
               batman:
                   configuration:
                       retries: 3
                   strategy_partition: 2
               catwoman:
                   configuration:
                       retries: 3
                   strategy_partition: 2

    consumers:
        consumer1:
            configuration:
                metadata.broker.list: '127.0.0.1'
                group.id: 'myConsumerGroup'
                enable.auto.commit: 0
            topicConfiguration:
                auto.offset.reset: 'smallest'
            timeout_consuming_queue: 200
            topics:
                - batman
                - catwoman
```

Note that we decided to use the high level consumer. 
So you can set the "group.id" option in the consumer configuration.

```yaml
configuration:
  metadata.broker.list: '127.0.0.1'
  group.id: 'myConsumerGroup'
```

For the producers, we have one topic configuration for each topic:
```yaml
 topics:
   batman:
       configuration:
           retries: 3
       strategy_partition: '2'
   catwoman:
       configuration:
           retries: 3
       strategy_partition: '2'
 ```

Whereas for the consumers, we have one topic configuration for all topics:
```yaml
topicConfiguration:
    auto.offset.reset: 'smallest'
timeout_consuming_queue: 200
topics:
    - batman
    - catwoman
 ```
 
### Producer

A producer will be used to send messages to the server.

In the Kafka Model, messages are sent to topics partitioned and stored on brokers.
This means that in the configuration for a producer you will have to specify the brokers and the topics.
You can optionnaly configure the log level and the strategy partitioner.

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

### Consumer

A consumer will be used to get a message from different topics.
You can choose to set only one topic by consumer.

In the Kafka Model, messages are consumed from topics partitioned and stored on brokers.
This means that for a consumer you will have to specify the brokers and topics in the configuration.

To consume messages, you will have to use the `consume` method to consume a message:
```php
$consumer->consume();
```
The messages will be automatically committed except if there is an error.
But you can choose not to do it by adding an argument as following:
```php
$consumer->consume(false);
```

*Note:* Setting $consumer->consume(false) boost consuming performance, avoiding to commit every single kafka consumed message.

You can decide to commit manually your message with:
```php
$consumer->commit();
```

It will commit the last consumed message.

It will give you an object `\RdKafka\Message` with information about the message : payload, topic, or partition for instance.
It is the `\RdKafka\Message` from the [RdKafka extension](https://arnaud-lb.github.io/php-rdkafka/phpdoc/book.rdkafka.html).

In case there is no more message, it will give you a _No more message_ string.
In case there is a time out, it will give you a _Time out_ string.

#### Consuming from CLI

You can use your own service to process Kakfa topic messages and run it from Symfony special command provided by us.

At first you have to create your own service implementing MessageHandlerInterface. For example:

```php
<?php

namespace AppBundle\Service;

use M6Web\Bundle\KafkaBundle\Handler\MessageHandlerInterface;
use RdKafka\Message;

class TestConsumer implements MessageHandlerInterface
{

    public function process(Message $message)
    {
        echo $message->payload. PHP_EOL;
    }

}
```

Then you could start consuming messages executing this command with your consumer and service name:

```
php bin/console kafka:consume you_consumer_name "AppBundle\Service\TestConsumer"
```

Also you can add --auto-commit option to enable auto commit in every consumed message.

*Note:* If you aren't using *Symfony Automatic Service Loading*, introduced in Symfony 3.3, you have to put in handler option your service name instead of FQCN.

This Symfony command provide *signal management* too, handling SIGQUIT, SIGTERM and SIGINT to do a gracefully shutdown, committing last processed message and exiting.

### Exceptions list
- EntityNotSetException
- KafkaException
- LogLevelNotSetException
- NoBrokerSetException
