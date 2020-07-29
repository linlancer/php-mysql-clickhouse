<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/18
 * Time: 16:39
 */

namespace LinLancer\PhpMySQLClickhouse\Test\Cache;

use Doctrine\Common\Cache\PredisCache;
use LinLancer\PhpMySQLClickhouse\Cache\DatabaseMappingRules;
use LinLancer\PhpMySQLClickhouse\Clickhouse\ClickhouseClient;
use PHPUnit\Framework\TestCase;
use Predis\Client;

class DatabaseMappingRulesTest extends TestCase
{

    public function getObj()
    {
        $config = include __dir__.'/../../config/clickhouse.php';
        $client = new Client($config['redis']);
        $cache = new PredisCache($client);
        $clickhouseConfig = $config['clickhouse_database'];
        $conf = [
            'host' => $clickhouseConfig['host'],
            'port' => $clickhouseConfig['port'],
            'username' => $clickhouseConfig['user'],
            'password' => $clickhouseConfig['password']
        ];

        $client = new ClickhouseClient($conf);
        return new DatabaseMappingRules($config, $cache, $client);
    }

    public function testGetDatabases()
    {
        $obj = $this->getObj();
    }

    public function testGetTables()
    {

    }
}
