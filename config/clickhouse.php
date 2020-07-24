<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/18
 * Time: 15:22
 */
return [
    //mysql源数据库配置
    'mysql_database' => [
        'host' => '192.168.66.33',
        'port' => 3306,
        'user' => 'reader',
        'password' => 'Ch_123456.',
        //要同步的数据表 格式：database.table
        'tables' => [
            'clickhouse_source.clickhouse_test',
        ],
    ],
    //clickhouse 数据源
    'clickhouse_database' => [
        'host' => '192.168.99.30',
        'port' => 8123,
        'user' => 'default',
        'password' => '',
    ],
    //要更新的数据库
    'schemas' => [
        'clickhouse_source',
    ],
    //要同步的数据表
    'tables' => [
        'clickhouse_test',//如果多个库中含有相同表名 加入数据库名以区分
    ],
    /**
     * 如果要指定源表更新到目标表的话 在这里指定映射 更复杂的映射
     * 这里键名为源表  键值为实现MySQLTableInterface接口的类名
     */
    'table_mapping' => [

    ],
    'redis' => [
        'host' => '192.168.66.188',
        'password' => 'Hexin188@2019',
        'port' => 6379,
        'database' => 3,
    ]
];