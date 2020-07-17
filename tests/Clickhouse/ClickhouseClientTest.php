<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 14:11
 */

namespace LinLancer\PhpMySQLClickhouse\Test\Clickhouse;

use ClickHouseDB\Statement;
use LinLancer\PhpMySQLClickhouse\Clickhouse\ClickhouseClient;
use PHPUnit\Framework\TestCase;

class ClickhouseClientTest extends TestCase
{
    public function getClickhouse()
    {
        $config = [
            'host' => '192.168.99.30',
            'port' => '8123',
            'username' => 'default',
            'password' => ''
        ];
        return new ClickhouseClient($config);
    }
    public function testIsExist()
    {
        $clickhouse = $this->getClickhouse();
        $this->assertInstanceOf(ClickhouseClient::class, $clickhouse);
        $result = $clickhouse->isExist('purchase', 'aaa');
        $this->assertIsBool($result);
    }

    public function testQuery()
    {
        $sql = 'show databases;';
        $clickhouse = $this->getClickhouse();
        $result = $clickhouse->query($sql);
        $this->assertInstanceOf(Statement::class, $result);
        var_dump($result->rawData());
    }

    public function testGetTableDefinition()
    {
        $clickhouse = $this->getClickhouse();
        $result = $clickhouse->getTableDefinition('test', 'hexin_product');
        var_dump($result);
    }
}
