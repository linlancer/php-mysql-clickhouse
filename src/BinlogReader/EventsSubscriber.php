<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/16
 * Time: 19:21
 */

namespace LinLancer\PhpMySQLClickhouse\BinlogReader;


use LinLancer\PhpMySQLClickhouse\Handler\InsertRowHandler;
use MySQLReplication\Event\DTO\EventDTO;
use MySQLReplication\Event\EventSubscribers;

class EventsSubscriber extends EventSubscribers
{
    /**
     * @param EventDTO $event (your own handler more in EventSubscribers class )
     */
    public function allEvents(EventDTO $event): void
    {
        // all events got __toString() implementation
        if ($event->getType() !== 'heartbeat') {
//            echo $event;
            // all events got JsonSerializable implementation
            //echo json_encode($event, JSON_PRETTY_PRINT);
            (new InsertRowHandler($event))->parseEvent();
//            echo 'Memory usage ' . round(memory_get_usage() / 1048576, 2) . ' MB' . PHP_EOL;
            // save event for resuming it later
            MySQLBinlogReader::save($event->getEventInfo()->getBinLogCurrent());
        }


    }
}