<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/16
 * Time: 20:09
 */
include __DIR__ .'/../vendor/autoload.php';

use LinLancer\PhpMySQLClickhouse\BinlogReader\MySQLBinlogReader;
use LinLancer\PhpMySQLClickhouse\Benchmark;

$config = require_once(__DIR__ .'/../config/clickhouse.php');
$client = new \Predis\Client($config['redis']);
$cache = new \Doctrine\Common\Cache\PredisCache($client);
if (false)
    $cache->delete('database:binlog:position');
Benchmark::log('【开启mysql-clickhouse自动同步组件】');
MySQLBinlogReader::run($config, $cache);