<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 11:30
 */

namespace LinLancer\PhpMySQLClickhouse\Handler;


class InsertRowHandler extends BaseEventHandler
{

    public function parseSql(array $values): string
    {
        $table = $this->reader->getTableRules()->getMysqlTable($this->db, $this->table);

        $dataGroup = [];
        foreach ($values as $row) {
            $data = [];
            foreach ($row as $key => $value) {
                if ($this->checkClickhouseColumn($this->db, $this->table, $key) === false)
                    continue;
                $sourceType = $table->getColumn($key)->getType()->getName();
                $data[$key] = $this->convertValue($value, $sourceType);
            }

            !empty($data) && $dataGroup[] = $data;
        }
        return empty($dataGroup) ? false : $this->insertSql($this->db, $this->table, $dataGroup);
    }
}