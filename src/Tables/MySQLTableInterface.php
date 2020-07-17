<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 10:26
 */

namespace LinLancer\PhpMySQLClickhouse\Tables;


interface MySQLTableInterface
{
    const SOURCE_DB = '';

    const SOURCE_TABLE = '';

    const SOURCE_CONN = '';

    const TARGET_DB = '';

    const TARGET_TABLE = '';

    const TARGET_CONN = '';

    const FIELDS_MAPPING = [];

}