<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 14:38
 */

namespace LinLancer\PhpMySQLClickhouse\Clickhouse;


class TableDefinitionParser
{
    protected $rawDefinition;

    protected $arrayDefinition;

    protected $database;

    protected $table;

    protected $engine;

    protected $fields = [];

    public function __construct($sql)
    {
        $this->rawDefinition = $sql;
        $this->arrayDefinition = $this->parseToArray();
        $this->parseFields();
    }

    public function parseToArray()
    {
        $reg = '/CREATE\s+TABLE\s+(IF NOT EXISTS\s+)?((?P<database>\w+)\.)?(?P<table>\w+)\s+(ON CLUSTER \w+\s+)?\(\s?(?P<fields>\s+`(.*))+\)\s+ENGINE\s+=\s+(?P<engine>\w+)/s';
        if (preg_match_all($reg, $this->rawDefinition, $match))
            return $match;
        return [];
    }

    public function getDatabase()
    {
        return $this->arrayDefinition['database'][0] ?? '';
    }

    public function getTable()
    {
        return $this->arrayDefinition['table'][0] ?? '';
    }

    public function getEngine()
    {
        return $this->arrayDefinition['engine'][0] ?? '';
    }

    public function getFields()
    {
        return $this->fields;
    }

    public function getColumnType(string $field)
    {
        return $this->fields[$field] ?? false;
    }

    private function parseFields()
    {
        $fields = $this->arrayDefinition['fields'][0] ?? '';
        $fieldsArr = explode(PHP_EOL, $fields);
        $fieldsArr = array_filter($fieldsArr);
        foreach ($fieldsArr as $field) {
            $field = substr(trim($field), 0, -1);
            $reg = '/`(?P<column>\w+)`\s+(?P<type>[\w\(\),]+)/';
            if (preg_match_all($reg, $field, $match)) {
                $this->fields[$match['column'][0]] = $match['type'][0];
            }

        }
    }


}