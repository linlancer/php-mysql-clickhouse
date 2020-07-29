<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/28
 * Time: 20:04
 */

namespace LinLancer\PhpMySQLClickhouse\Test\BinlogReader;

use Doctrine\Common\Cache\PredisCache;
use LinLancer\PhpMySQLClickhouse\BinlogReader\MySQLBinlogReader;
use PHPUnit\Framework\TestCase;
use Predis\Client;

class MySQLBinlogReaderTest extends TestCase
{

    public function getConfig()
    {
        return require_once __dir__.'/../../config/clickhouse.php';
    }

    public function getCache()
    {
        $config = $this->getConfig();
        $client = new Client($config['redis']);
        return new PredisCache($client);
    }
    public function testRun()
    {
        MySQLBinlogReader::run($this->getConfig(), $this->getCache());
    }
}
