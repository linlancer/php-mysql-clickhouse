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

    public function getTableDefinition($schema, $table)
    {
        $createTableSql = $this->client->database($schema)->showCreateTable($table);
        $this->parseCreateTableSql($createTableSql);
    }

    private function parseCreateTableSql($sql)
    {

    }
}