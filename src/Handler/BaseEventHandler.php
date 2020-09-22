<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 11:19
 */

namespace LinLancer\PhpMySQLClickhouse\Handler;


use LinLancer\PhpMySQLClickhouse\Benchmark;
use LinLancer\PhpMySQLClickhouse\BinlogReader\MySQLBinlogReader;
use LinLancer\PhpMySQLClickhouse\Clickhouse\TypeMapping;
use LinLancer\PhpMySQLClickhouse\Task\RedisQueue;
use MySQLReplication\Event\DTO\EventDTO;
use MySQLReplication\Event\DTO\RowsDTO;

abstract class BaseEventHandler
{
    const QUEUE_PREFIX = 'clickhouse:redis:queue:';
    /**
     * @var string
     */
    protected $db;
    /**
     * @var string
     */
    protected $table;
    /**
     * @var MySQLBinlogReader
     */
    protected $reader;

    /**
     * @var RedisQueue
     */
    protected $queue;

    /**
     * @var EventDTO|RowsDTO
     */
    protected $event;

    public function __construct(EventDTO $event)
    {
        $this->event = $event;
        $this->reader = MySQLBinlogReader::getInstance();
        $this->queue = new RedisQueue($this->reader->getConfig()['redis']);
        $this->initEvent();
    }

    public function initEvent()
    {
        if ($this->event instanceof RowsDTO) {
            $this->db = $this->event->getTableMap()->getDatabase();
            $this->table = $this->event->getTableMap()->getTable();
        }
    }

    public function handle() {
        $values = $this->event->getValues();
        $tail = $this->db . ':' . $this->table;
        $sql = $this->parseSql($values);
        $delay = time() - $this->event->getEventInfo()->getTimestamp();
        $date = $this->event->getEventInfo()->getDateTime();
        $content = sprintf('【主库时间】%s【当前延迟】 %s 秒', $date, $delay);
        Benchmark::process($content);
        $this->queue->push(self::QUEUE_PREFIX . $tail, $sql[0]);
    }

    public function updateSql($db, $table, $changes, $conditons)
    {
        $changesGroup = [];
        foreach ($changes as $k => $v) {
            $changesGroup[] = $this->quoteField($k) . ' = ' . $v;
        }
        $columns = implode(', ', $changesGroup);
        if (is_array($conditons)) {
            $conditonsGroup = [];
            foreach ($conditons as $k => $v) {
                $conditonsGroup[] = $this->quoteField($k) . ' = ' . $v;
            }
            $condition = implode('AND ', $conditonsGroup);
        } else {
            $condition = $conditons;
        }
        $sql = <<<CLICKHOUSE_UPDATE_PATTERN
            ALTER TABLE `$db`.`$table` UPDATE $columns WHERE $condition;
CLICKHOUSE_UPDATE_PATTERN;
        return $sql;
    }

    public function insertSql($db, $table, $data)
    {
        $columns = [];
        $values = [];
        foreach ($data as $insert) {
            $columns = array_keys($insert);
            $values[] = '(' . implode(',', array_values($insert)) . ')';
        }
        $columns = '( `' . implode('`, `', $columns) . '` )';
        $values = implode(',', $values);

        $sql = <<<CLICKHOUSE_UPDATE_PATTERN
            INSERT INTO `$db`.`$table` $columns VALUES $values;
CLICKHOUSE_UPDATE_PATTERN;
        return $sql;
    }

    public function deleteSql($db, $table, $conditons)
    {
        if (is_array($conditons)) {
            $conditonsGroup = [];
            foreach ($conditons as $k => $v) {
                $conditonsGroup[] = $k . ' = ' . $v;
            }
            $condition = implode('AND ', $conditonsGroup);
        } else {
            $condition = $conditons;
        }
        $sql = <<<CLICKHOUSE_UPDATE_PATTERN
            ALTER TABLE `$db`.`$table` DELETE WHERE $condition;
CLICKHOUSE_UPDATE_PATTERN;
        return $sql;
    }

    private function quoteField($field)
    {
        return sprintf('`%s`', $field);
    }

    public function convertValue($value, $sourceType)
    {
        return TypeMapping::convert($value, $sourceType);
    }

    /**
     * clickhouse执行对应语句
     * @param $sql
     * @return bool|\ClickHouseDB\Statement
     */
    public function clickhouseQuery($sql)
    {
        if (is_array($sql)) {
            foreach ($sql as $unit) {
                $this->reader->getClickhouse()->query($unit);
            }
        } else {
            return $this->reader->getClickhouse()->query($sql);
        }
    }

    /**
     * 检查clickhouse中是否存在对应字段
     * @param $db
     * @param $table
     * @param $column
     * @return bool
     */
    public function checkClickhouseColumn($db, $table, $column)
    {
        list ($db, $table) = $this->mapToClickhouse($db, $table);
        $tableDetail = $this->reader->getTableRules()->getClickhouseTable($db, $table);
        return boolval($tableDetail->getColumnType($column));
    }

    public function mapToClickhouse($db, $table)
    {
        $rules = $this->reader->getTableRules()->getMapingRules();
        if (!isset($rules[$db . '.' . $table])) {
            return [$db, $table];
        } else {
            return explode('.', $rules[$db . '.' . $table]);
        }
    }

    public function mapToMysql($db, $table)
    {
        $rules = $this->reader->getTableRules()->getMapingRules();
        $rules = array_reverse($rules);
        if (!isset($rules[$db . '.' . $table])) {
            return [$db, $table];
        } else {
            return explode('.', $rules[$db . '.' . $table]);
        }
    }

}