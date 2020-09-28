<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 10:32
 */

namespace LinLancer\PhpMySQLClickhouse\Clickhouse;


use Doctrine\DBAL\Types\Types;

class TypeMapping
{
    const MYSQL_BIT = 'bit';
    const MYSQL_TINYINT = 'tinyint';
    const MYSQL_SMALLINT = 'smallint';
    const MYSQL_MEDIUMINT = 'mediumint';
    const MYSQL_INT = 'int';
    const MYSQL_BIGINT = 'bigint';
    const MYSQL_DECIMAL = 'decimal';
    const MYSQL_FLOAT = 'float';
    const MYSQL_DOUBLE = 'double';
    const MYSQL_DATE = 'date';
    const MYSQL_DATETIME = 'datetime';
    const MYSQL_TIMESTAMP = 'timestamp';
    const MYSQL_TIME = 'time';
    const MYSQL_YEAR = 'year';
    const MYSQL_CHAR = 'char';
    const MYSQL_VARCHAR = 'varchar';
    const MYSQL_TINYTEXT = 'tinytext';
    const MYSQL_TEXT = 'text';
    const MYSQL_MEDIUMTEXT = 'mediumtext';
    const MYSQL_LONGTEXT = 'longtext';
    const MYSQL_JSON = 'json';

    const CLICKHOUSE_DATE = 'Date';
    const CLICKHOUSE_DATETIME = 'DateTime';
    const CLICKHOUSE_ENUM = 'Enum';
    const CLICKHOUSE_FLOAT32 = 'Float32';
    const CLICKHOUSE_FLOAT64 = 'Float64';
    const CLICKHOUSE_INT8 = 'Int8';
    const CLICKHOUSE_BOOLEAN = 'Int8';
    const CLICKHOUSE_INT16 = 'Int16';
    const CLICKHOUSE_INT32 = 'Int32';
    const CLICKHOUSE_INT64 = 'Int64';
    const CLICKHOUSE_UINT8 = 'UInt8';
    const CLICKHOUSE_UINT16 = 'UInt16';
    const CLICKHOUSE_UINT32 = 'UInt32';
    const CLICKHOUSE_UINT64 = 'UInt64';
    const CLICKHOUSE_FIXED_STRING = 'FixedString(N)';
    const CLICKHOUSE_ARRAY = 'Array(T)';
    const CLICKHOUSE_STRING = 'String';
    const CLICKHOUSE_DECIMAL = 'Decimal';

    const MAPPING = [
        Types::BOOLEAN => self::CLICKHOUSE_BOOLEAN,
        self::MYSQL_TINYINT => self::CLICKHOUSE_INT8,
        self::MYSQL_SMALLINT => self::CLICKHOUSE_INT16,
        self::MYSQL_MEDIUMINT => self::CLICKHOUSE_INT32,
        self::MYSQL_INT => self::CLICKHOUSE_INT32,
        self::MYSQL_BIGINT => self::CLICKHOUSE_INT64,
        self::MYSQL_DECIMAL => self::CLICKHOUSE_DECIMAL,
        self::MYSQL_FLOAT => self::CLICKHOUSE_FLOAT32,
        self::MYSQL_DOUBLE => self::CLICKHOUSE_FLOAT64,
        self::MYSQL_DATE => self::CLICKHOUSE_DATE,
        self::MYSQL_DATETIME => self::CLICKHOUSE_DATETIME,
        self::MYSQL_TIMESTAMP => self::CLICKHOUSE_DATETIME,
        self::MYSQL_TIME => self::CLICKHOUSE_STRING,
        self::MYSQL_YEAR => self::CLICKHOUSE_UINT16,
        self::MYSQL_CHAR => self::CLICKHOUSE_FIXED_STRING,
        self::MYSQL_VARCHAR => self::CLICKHOUSE_STRING,
        self::MYSQL_TINYTEXT => self::CLICKHOUSE_STRING,
        self::MYSQL_TEXT => self::CLICKHOUSE_STRING,
        self::MYSQL_MEDIUMTEXT => self::CLICKHOUSE_STRING,
        self::MYSQL_LONGTEXT => self::CLICKHOUSE_STRING,
        self::MYSQL_JSON => self::CLICKHOUSE_ARRAY,
    ];

    public static function convert($value, $type, $unsign = false)
    {
        switch ($type) {
            case Types::DATE_MUTABLE:
                $value = sprintf('toDateOrNull(\'%s\')', $value);
                break;
            case Types::DATETIME_MUTABLE:
                $value = sprintf('toDateTimeOrNull(\'%s\')', $value);
                break;
            case Types::STRING:
            case Types::DECIMAL:
            case Types::TEXT:
            case Types::JSON:
                if (strpos($value, '\'') !== false)
                    $value = str_replace('\'', '\\\'', $value);
                $value = sprintf('\'%s\'', $value);
                break;
            case Types::BIGINT:
                $value = intval($value);
                $value = $unsign ? sprintf('toUInt64(%d)', $value) : $value;
                break;
            case Types::INTEGER:
                $value = intval($value);
                $value = $unsign ? sprintf('toUInt32(%d)', $value) : $value;
                break;
            case Types::SMALLINT:
                $value = intval($value);
                $value = $unsign ? sprintf('toUInt16(%d)', $value) : $value;
                break;
            case Types::BOOLEAN:
                $value = intval($value);
                $value = $unsign ? sprintf('toUInt8(%d)', $value) : $value;
                break;
            case Types::FLOAT:
                $value = floatval($value);
                break;
            default:
                break;
        }
        return $value;
    }

}