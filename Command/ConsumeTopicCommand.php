<?php

declare(ticks = 1);

namespace M6Web\Bundle\KafkaBundle\Command;

use M6Web\Bundle\KafkaBundle\Manager\ConsumerManager;
use M6Web\Bundle\KafkaBundle\Handler\MessageHandlerInterface;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class ConsumeTopicCommand extends ContainerAwareCommand
{
    protected $shutdown;

    protected function configure()
    {
        $this
            ->setName('m6web:kafka:consume')
            ->setDescription('Consume command to process kafka topics')
            ->addArgument('consumer', InputArgument::REQUIRED, 'Consumer name')
            ->addArgument('handler', InputArgument::REQUIRED, 'Handler service name')
            ->addOption('auto-commit', null, InputOption::VALUE_NONE, 'Auto commit enabled?')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $container = $this->getContainer();
        $prefixName = $container->getParameter('m6web_kafka.services_name_prefix');

        $consumer = $input->getArgument('consumer');
        $handler = $input->getArgument('handler');
        $autoCommit = $input->getOption('auto-commit');

        /**
         * @var ConsumerManager $topicConsumer
         */
        $topicConsumer = $container->get(sprintf('%s.consumer.%s', $prefixName, $consumer));
        if (!$topicConsumer) {
            throw new \Exception(sprintf("TopicConsumer with name '%s' is not defined", $consumer));
        }

        /**
         * @var MessageHandlerInterface $messageHandler
         */
        $messageHandler = $container->get($handler);
        if (!$messageHandler) {
            throw new \Exception(sprintf("Message Handler with name '%s' is not defined", $handler));
        }

        $output->writeln('<comment>Waiting for partition assignment... (make take some time when quickly re-joining the group after leaving it.)'.PHP_EOL.'</comment>');

        $this->registerSigHandlers();

        while (true) {
            $message = $topicConsumer->consume($autoCommit);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $messageHandler->process($message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    $output->writeln('<question>No more messages; will wait for more</question>');
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $output->writeln('<question>Timed out</question>');
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }

            if($this->shutdown) {
                $output->writeln('<question>Shuting down...</question>');
                if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                    $topicConsumer->commit();
                }

                break;
            }
        }

        $output->writeln('<info>End consuming topic gracefully</info>');
    }

    private function registerSigHandlers()
    {
        if(!function_exists('pcntl_signal')) {
            return;
        }

        pcntl_signal(SIGTERM, [$this, 'shutdownFn']);
        pcntl_signal(SIGINT, [$this, 'shutdownFn']);
        pcntl_signal(SIGQUIT, [$this, 'shutdownFn']);
    }

    public function shutdownFn()
    {
        $this->shutdown = true;
    }

}
