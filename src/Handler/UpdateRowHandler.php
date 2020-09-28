<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 11:30
 */

namespace LinLancer\PhpMySQLClickhouse\Handler;


use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;

class UpdateRowHandler extends BaseEventHandler
{

    public function parseSql(array $values):string
    {
        /**
         * @var Table $table
         */
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
                /**
                 * @var Column $column
                 */
                $column = $table->getColumn($key);
                $sourceType = $column->getType()->getName();
                $unsign = $column->getUnsigned();
                if ($beforeValue !== $after[$key])
                    $change[$key] = $this->convertValue($after[$key], $sourceType, $unsign);
                if ($primaryKeyName == $key)
                    $where[$key] = $this->convertValue($before[$key], $sourceType, $unsign);
            }
            $sql = $this->updateSql($this->db, $this->table, $change, $where);
            !empty($change) && $sqlGroup[] = $sql;
        }
        return empty($sqlGroup) ? false : ($sqlGroup[0] ?? '');
    }

}