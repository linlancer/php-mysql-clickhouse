<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/16
 * Time: 19:21
 */

namespace LinLancer\PhpMySQLClickhouse\BinlogReader;


use LinLancer\PhpMySQLClickhouse\Handler\DeleteRowHandler;
use LinLancer\PhpMySQLClickhouse\Handler\InsertRowHandler;
use LinLancer\PhpMySQLClickhouse\Handler\UpdateRowHandler;
use MySQLReplication\Definitions\ConstEventsNames;
use MySQLReplication\Event\DTO\EventDTO;
use MySQLReplication\Event\EventSubscribers;

class EventsSubscriber extends EventSubscribers
{
    /**
     * @param EventDTO $event (your own handler more in EventSubscribers class )
     */
    public function allEvents(EventDTO $event): void
    {
        $eventType = $event->getType();
        switch ($eventType) {
            case ConstEventsNames::DELETE:
                echo '【start handling delete events】'.PHP_EOL;
                (new DeleteRowHandler($event))->handle();
                break;
            case ConstEventsNames::UPDATE:
                echo '【start handling update events】'.PHP_EOL;
                (new UpdateRowHandler($event))->handle();
                break;
            case ConstEventsNames::WRITE:
                echo '【start handling insert events】'.PHP_EOL;
                (new InsertRowHandler($event))->handle();
                break;
            case ConstEventsNames::QUERY:
                echo '【fetch ddl and update cache soon】'.PHP_EOL;
                MySQLBinlogReader::getInstance()->updateTableCache();
                break;
            default:
                break;
        }
        MySQLBinlogReader::save($event->getEventInfo()->getBinLogCurrent());
    }
}