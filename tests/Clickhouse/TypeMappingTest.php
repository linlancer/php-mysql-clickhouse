<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/10/29
 * Time: 10:32
 */

namespace LinLancer\PhpMySQLClickhouse\Test\Clickhouse;

use LinLancer\PhpMySQLClickhouse\Clickhouse\TypeMapping;
use PHPUnit\Framework\TestCase;

class TypeMappingTest extends TestCase
{

    public function testConvert()
    {
        $value = TypeMapping::convert(null, 'decimal');
        $this->assertNull($value);
        $value = TypeMapping::convert(8, 'integer', true);
        $this->assertIsInt($value);
        $value = TypeMapping::convert(0.8, 'float');
        $this->assertIsFloat($value);
        $value = TypeMapping::convert('0.80', 'decimal');
        $this->assertIsString($value);

    }
}
