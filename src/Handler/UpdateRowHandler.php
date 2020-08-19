<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 11:30
 */

namespace LinLancer\PhpMySQLClickhouse\Handler;


class UpdateRowHandler extends BaseEventHandler
{
    public function handle()
    {
        $values = $this->event->getValues();
        $sql = $this->parseSql($values);
        $sql && $this->clickhouseQuery($sql);
    }

    private function parseSql(array $values)
    {
        $table = $this->reader->getTableRules()->getMysqlTable($this->db, $this->table);
        $primaryKey = $table->getPrimaryKey()->getColumns();
        $primaryKeyName = reset($primaryKey);
        $sqlGroup = [];
        foreach ($values as $value) {
            $before = $value['before'];
            $after = $value['after'];
            $change = [];
            $where = [];
            foreach ($before as $key => $beforeValue) {
                if ($this->checkClickhouseColumn($this->db, $this->table, $key) === false)
                    continue;
                $sourceType = $table->getColumn($key)->getType()->getName();
                if ($beforeValue !== $after[$key])
                    $change[$key] = $this->convertValue($after[$key], $sourceType);
                if ($primaryKeyName == $key)
                    $where[$key] = $this->convertValue($before[$key], $sourceType);
            }
            $sql = $this->updateSql($this->db, $this->table, $change, $where);
            !empty($change) && $sqlGroup[] = $sql;
        }
        return empty($sqlGroup) ? false : $sqlGroup;
    }

}