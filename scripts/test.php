<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/16
 * Time: 20:09
 */
include __DIR__ .'/../vendor/autoload.php';

use LinLancer\PhpMySQLClickhouse\BinlogReader\MySQLBinlogReader;
use MySQLReplication\Config\ConfigBuilder;

$config = new ConfigBuilder();
MySQLBinlogReader::stream($config);