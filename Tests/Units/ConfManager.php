<?php
namespace M6Web\Bundle\KafkaBundle\Tests\Units;

use M6Web\Bundle\KafkaBundle\ConfManager as Base;
use M6Web\Bundle\KafkaBundle\Tests\Units\BaseUnitTest;

/**
 * Class Conf
 * @package M6Web\Bundle\KafkaBundle\Tests\Units
 *
 * A test class to test Conf class
 */
class ConfManager extends BaseUnitTest
{
    /**
     * @return void
     */
    public function testShouldSetEntityConf()
    {
        $this
            ->given(
                $conf = new Base(),
                $rdKafkaConfMock = $this->getRdKafkaConfMock()
            )
            ->if($result = $conf->getConf($rdKafkaConfMock, ['conf' => 'value']))
            ->then
                ->object($result)
                    ->isInstanceOf('\RdKafka\Conf')
                ->mock($rdKafkaConfMock)
                    ->call('set')
                        ->once()
        ;
    }

    /**
     * @return \mock\RdKafka\Conf
     */
    protected function getRdKafkaConfMock()
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Conf();
        $mock->getMockController()->set = true;

        return $mock;
    }
}
