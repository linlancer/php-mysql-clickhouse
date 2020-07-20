<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 13:54
 */

namespace LinLancer\PhpMySQLClickhouse\Clickhouse;


use ClickHouseDB\Client;

class ClickhouseClient
{
    protected $config = [];

    protected $db;

    protected $client;

    protected $connectTimeOut;

    protected $timeOut;

    public function __construct($config, $settings = [])
    {
        $this->config = $config;
        $this->client = new Client($this->config, $settings);
    }

    /**
     * @param $sql
     * @return bool|\ClickHouseDB\Statement
     */
    public function query($sql)
    {
        try {
            $statement = $this->client->write($sql);
            return $statement;
        } catch (\Exception $e) {
            echo $e->getMessage();
            return false;
        }
    }

    public function isExist($schema, $table)
    {
        return boolval($this->client->isExists($schema, $table));
    }

    public function createDatabase($schema)
    {
        $sql = 'CREATE DATABASE IF NOT EXISTS %s';
        return $this->query(sprintf($sql, $schema));
    }

    /**
     * @param $schema
     * @param $table
     * @return TableDefinitionParser
     */
    public function getTableDefinition($schema, $table)
    {
        $createTableSql = $this->client->database($schema)->showCreateTable($table);
        return $this->parseCreateTableSql($createTableSql);
    }

    /**
     * @param $sql
     * @return TableDefinitionParser
     */
    private function parseCreateTableSql($sql)
    {
        $parser = new TableDefinitionParser($sql);
        return $parser;
    }
}