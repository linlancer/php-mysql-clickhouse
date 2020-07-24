<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/18
 * Time: 16:39
 */

namespace LinLancer\PhpMySQLClickhouse\Test\Cache;

use LinLancer\PhpMySQLClickhouse\Cache\DatabaseMappingRules;
use PHPUnit\Framework\TestCase;

class DatabaseMappingRulesTest extends TestCase
{

    public function getObj()
    {
        $config = include __dir__.'/../../config/clickhouse.php';
        return new DatabaseMappingRules($config);
    }

    public function testGetDatabases()
    {
        $obj = $this->getObj();
    }

    public function testGetTables()
    {

    }
}
