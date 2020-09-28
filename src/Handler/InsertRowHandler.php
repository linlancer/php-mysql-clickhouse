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
                $column = $table->getColumn($key);
                $sourceType = $column->getType()->getName();
                $unsign = $column->getUnsigned();
                $data[$key] = $this->convertValue($value, $sourceType, $unsign);
            }

            !empty($data) && $dataGroup[] = $data;
        }
        return empty($dataGroup) ? '' : $this->insertSql($this->db, $this->table, $dataGroup);
    }
}