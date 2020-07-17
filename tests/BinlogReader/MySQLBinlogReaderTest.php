<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/16
 * Time: 19:31
 */

namespace LinLancer\PhpMySQLClickhouse\Test\BinlogReader;

use LinLancer\PhpMySQLClickhouse\BinlogReader\MySQLBinlogReader;
use MySQLReplication\Config\ConfigBuilder;
use PHPUnit\Framework\TestCase;

class MySQLBinlogReaderTest extends TestCase
{

    public function testStream()
    {
        $config = new ConfigBuilder();
        MySQLBinlogReader::stream($config);
    }
}
