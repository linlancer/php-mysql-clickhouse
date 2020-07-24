<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/16
 * Time: 19:19
 */

namespace LinLancer\PhpMySQLClickhouse\BinlogReader;


use Doctrine\Common\Cache\Cache;
use MySQLReplication\BinLog\BinLogCurrent;
use MySQLReplication\Config\ConfigBuilder;
use MySQLReplication\MySQLReplicationFactory;

class MySQLBinlogReader
{
    const CACHE_KEY = 'database:binlog:position';

    /**
     * @var Cache
     */
    private static $cache;

    public static function setCache(Cache $cache)
    {
        self::$cache = $cache;
    }
    /**
     * @param BinLogCurrent $binLogCurrent
     */
    public static function save(BinLogCurrent $binLogCurrent): void
    {
        echo 'saving file:' . $binLogCurrent->getBinFileName() . ', position:' . $binLogCurrent->getBinLogPosition() . ' bin log position' . PHP_EOL;

        self::$cache->save(self::CACHE_KEY, serialize($binLogCurrent));
    }

    /**
     * @param ConfigBuilder $builder
     * @return ConfigBuilder
     */
    public static function startFromPosition(ConfigBuilder $builder): ConfigBuilder
    {
        if (!self::$cache->fetch(self::CACHE_KEY)) {
            return $builder;
        }
        /** @var BinLogCurrent $binLogCurrent */
        $binLogCurrent = unserialize(self::$cache->fetch(self::CACHE_KEY));

        echo 'starting from file:' . $binLogCurrent->getBinFileName() . ', position:' . $binLogCurrent->getBinLogPosition() . ' bin log position' . PHP_EOL;

        return $builder
            ->withBinLogFileName($binLogCurrent->getBinFileName())
            ->withBinLogPosition($binLogCurrent->getBinLogPosition());
    }

    public static function stream(ConfigBuilder $config)
    {
        $binLogStream = new MySQLReplicationFactory(
            self::startFromPosition($config)
                ->withUser('root')
                ->withHost('192.168.66.33')
                ->withPort(3306)
                ->withPassword('Hexin2007')
                ->withDatabasesOnly(['clickhouse_source'])
                ->withSlaveId(100)
                ->withHeartbeatPeriod(2)
                ->build()
        );
        $binLogStream->registerSubscriber(new EventsSubscriber);

        $binLogStream->run();
    }

    public static function run(ConfigBuilder $config)
    {

    }

    private static function initConfigBuilder(array $config)
    {

    }
}