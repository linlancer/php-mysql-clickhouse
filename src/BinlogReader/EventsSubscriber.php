<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/16
 * Time: 19:21
 */

namespace LinLancer\PhpMySQLClickhouse\BinlogReader;


use LinLancer\PhpMySQLClickhouse\Benchmark;
use LinLancer\PhpMySQLClickhouse\Handler\DeleteRowHandler;
use LinLancer\PhpMySQLClickhouse\Handler\InsertRowHandler;
use LinLancer\PhpMySQLClickhouse\Handler\UpdateRowHandler;
use MySQLReplication\Definitions\ConstEventsNames;
use MySQLReplication\Event\DTO\EventDTO;
use MySQLReplication\Event\DTO\QueryDTO;
use MySQLReplication\Event\EventSubscribers;

class EventsSubscriber extends EventSubscribers
{
    /**
     * @param EventDTO $event (your own handler more in EventSubscribers class )
     */
    public function allEvents(EventDTO $event): void
    {
        /**
         *
         */
        $eventType = $event->getType();
        switch ($eventType) {
            case ConstEventsNames::DELETE:
                Benchmark::start('开始处理删除事件');
                (new DeleteRowHandler($event))->handle();
                Benchmark::end('删除完成');
                break;
            case ConstEventsNames::UPDATE:
                Benchmark::start('开始处理更新事件');
                (new UpdateRowHandler($event))->handle();
                Benchmark::end('更新结束');
                break;
            case ConstEventsNames::WRITE:
                Benchmark::start('开始处理写入事件');
                (new InsertRowHandler($event))->handle();
                Benchmark::end('写入完成');
                break;
            case ConstEventsNames::QUERY:
                /**
                 * @var QueryDTO $event
                 */
                $query = $event->getQuery();
                if (stripos($query, 'BEGIN') !== 0) {
                    Benchmark::start('结构变更');
                    MySQLBinlogReader::getInstance()->updateTableCache();
                    Benchmark::end('表结构变更');
                }
                break;
            default:
                break;
        }
        MySQLBinlogReader::save($event->getEventInfo()->getBinLogCurrent());
    }
}