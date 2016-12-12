<?php
namespace M6Web\Bundle\KafkaBundle\Tests\Units\Event;

use atoum\test;
use M6Web\Bundle\KafkaBundle\Event\EventLog as Base;

/**
 * Class EventLog
 * @package M6Web\Bundle\KafkaBundle\Tests\Units\Event
 *
 * A class to test the EventLog
 */
class EventLog extends test
{
    /**
     * @return void
     */
    public function testShouldGetACorrectEventAfterConstruction()
    {
        $this
            ->if($event = new Base('consumer'))
            ->then
                ->string($event->getOrigin())
                    ->isEqualTo('consumer')
            ;
    }
}
