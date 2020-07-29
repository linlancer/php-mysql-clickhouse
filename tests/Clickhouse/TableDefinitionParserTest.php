<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 14:43
 */

namespace LinLancer\PhpMySQLClickhouse\Test\Clickhouse;

use LinLancer\PhpMySQLClickhouse\Clickhouse\TableDefinitionParser;
use PHPUnit\Framework\TestCase;

class TableDefinitionParserTest extends TestCase
{
    public function getParser()
    {
        $sql = <<<SQL
CREATE TABLE hexin_storage.hexin_erp_storage_stock
(
    `id` Int32, 
    `storage_id` Int32, 
    `product_id` Int32, 
    `spec_id` Int32, 
    `sku` String, 
    `comment` String, 
    `max_stock` Int32, 
    `min_stock` Int32, 
    `avg` String, 
    `avg_a` String, 
    `avg_b` String, 
    `avg_c` String, 
    `avg_d` String, 
    `avg_e` String, 
    `wait_in` Int32, 
    `stock` UInt32, 
    `stock_total` UInt32, 
    `occupy_num` Int32, 
    `purchase_day` Int32, 
    `stay_on` Int32, 
    `less_num` Int32, 
    `sending_num` Int32, 
    `online_num` Int32, 
    `receiving_num` Int32, 
    `picked_num` Int32, 
    `purchase_num` Int32, 
    `predict_num` Int32, 
    `suggest_num` Int32, 
    `tag_id` String, 
    `avg_f` String, 
    `avg_g` String, 
    `warning_pur_num` Int32, 
    `pur_num` Int32, 
    `no_warning_pur_num` Int32, 
    `create_time` Nullable(DateTime), 
    `update_time` Nullable(DateTime), 
    `status` Int8
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
SQL;
        return new TableDefinitionParser($sql);
    }
    public function testGetField()
    {
        $parser = $this->getParser();
        $type = $parser->getColumnType('avg_f');
        $this->assertIsString($type);
    }

    public function testGetDatabase()
    {
        $parser = $this->getParser();
        $database = $parser->getDatabase();
        $this->assertIsString($database);
    }

    public function testGetTable()
    {
        $parser = $this->getParser();
        $table = $parser->getTable();
        $this->assertIsString($table);
    }

    public function testGetFields()
    {
        $parser = $this->getParser();
        $fields = $parser->getFields();
        $this->assertIsArray($fields);
    }

    public function testParseToArray()
    {
        $parser = $this->getParser();
        $engine = $parser->getEngine();
        $this->assertIsString($engine);


    }
}
