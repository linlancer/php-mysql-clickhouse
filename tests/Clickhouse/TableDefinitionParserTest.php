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
CREATE TABLE test.hexin_erp_product
(
    `id` Int32, 
    `cate_id` Nullable(Int32), 
    `supplier_id` Nullable(Int32), 
    `brand_id` Nullable(Int32), 
    `product_cname` Nullable(String), 
    `product_ename` Nullable(String), 
    `product_status` Nullable(Int8), 
    `old_parent_sku` Nullable(String), 
    `parent_sku` Nullable(String), 
    `purchase_day` Nullable(Int32), 
    `purchaser` Nullable(String), 
    `purchase_minprice` Decimal(10, 2), 
    `purchase_maxprice` Decimal(10, 2), 
    `product_image` Nullable(String), 
    `purchase_link` Nullable(String), 
    `is_electric` Nullable(Int8), 
    `is_powder` Nullable(Int8), 
    `is_liquid` Nullable(Int8), 
    `is_magnetic` Nullable(Int8), 
    `is_tort` Nullable(Int8), 
    `is_knowledge` Nullable(Int8), 
    `material` Nullable(String), 
    `unit` Nullable(String), 
    `season` Nullable(String), 
    `apply_cname` Nullable(String), 
    `apply_ename` Nullable(String), 
    `apply_price` Decimal(10, 2), 
    `apply_code` Nullable(String), 
    `storage_id` Nullable(Int32), 
    `origin_country` Nullable(String), 
    `origin_country_code` Nullable(String), 
    `max_stock` Nullable(Int32), 
    `min_stock` Nullable(Int32), 
    `cost_price` Decimal(10, 2), 
    `out_box_single_weight` Decimal(10, 2), 
    `out_box_height` Decimal(10, 2), 
    `out_box_length` Decimal(10, 2), 
    `out_box_width` Decimal(10, 2), 
    `out_box_gross_weight` Decimal(10, 2), 
    `box_single_weight` Decimal(10, 2), 
    `box_height` Decimal(10, 2), 
    `box_length` Decimal(10, 2), 
    `box_width` Decimal(10, 2), 
    `box_gross_weight` Decimal(10, 2), 
    `checker` Nullable(String), 
    `check_status` Nullable(Int8), 
    `check_time` Nullable(Int32), 
    `check_info` Nullable(String), 
    `developer` Nullable(String), 
    `create_time` Nullable(Int32), 
    `modify_time` Nullable(Int32), 
    `del_flag` Nullable(Int8), 
    `product_sub_images` Nullable(String), 
    `property_data` Nullable(String), 
    `pid` Nullable(String), 
    `description` Nullable(String), 
    `unsale_time` Nullable(Int32), 
    `comment` Nullable(String), 
    `comment2` Nullable(String), 
    `tag_id` String, 
    `product_link` String, 
    `fabric_weight` Nullable(Int32), 
    `size_img_str` Nullable(String), 
    `is_model` Nullable(Int8), 
    `is_real` Nullable(Int8), 
    `model_from` Nullable(Int8), 
    `real_from` Nullable(Int8), 
    `is_order` Nullable(Int8), 
    `first_arrive_time` Nullable(Int32), 
    `first_order_num` Nullable(String), 
    `start_order_num` Nullable(String), 
    `other_comment` Nullable(String), 
    `is_paste` Nullable(Int8), 
    `un_sale_reason` Nullable(String), 
    `is_new` Nullable(Int8), 
    `package_size` Nullable(String), 
    `publish_time` Int32, 
    `tort_reason` String, 
    `tort_time` Int32, 
    `size_adress` String, 
    `edit_status` Nullable(Int8), 
    `original_cate_id` Int32, 
    `edit_time` Int32, 
    `warehouse_entry_time` Int32, 
    `tag_attribute` Int32, 
    `image_tag` String, 
    `version` Int32
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
        $type = $parser->getColumnType('version');
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
