<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 11:30
 */

namespace LinLancer\PhpMySQLClickhouse\Handler;


class DeleteRowHandler extends BaseEventHandler
{

    public function parseSql(array $values)
    {
        $table = $this->reader->getTableRules()->getMysqlTable($this->db, $this->table);
        $primaryKey = $table->getPrimaryKey()->getColumns();
        $primaryKeyName = reset($primaryKey);
        $sqlGroup = [];

        foreach ($values as $row) {
            $where = [];
            foreach ($row as $key => $value) {
                $sourceType = $table->getColumn($key)->getType()->getName();

                if ($primaryKeyName == $key)
                    $where[$key] = $this->convertValue($value, $sourceType);
            }
            $sql = $this->deleteSql($this->db, $this->table, $where);
            !empty($where) && $sqlGroup[] = $sql;
        }
        return empty($sqlGroup) ? false : $sqlGroup;
    }
}