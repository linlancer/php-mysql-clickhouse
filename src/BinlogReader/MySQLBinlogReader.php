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
    /**
     * @var array
     */
    private static $config;

    /**
     * @var Cache
     */
    private static $cache;
    /**
     * @var string
     */
    private static $fileAndPath;

    /**
     * @return string
     */
    private static function getFileAndPath(): string
    {
        if (null === self::$fileAndPath) {
            self::$fileAndPath = sys_get_temp_dir() . '/bin-log-replicator-last-position';
        }
        return self::$fileAndPath;
    }

    /**
     * @param BinLogCurrent $binLogCurrent
     */
    public static function save(BinLogCurrent $binLogCurrent): void
    {
        echo 'saving file:' . $binLogCurrent->getBinFileName() . ', position:' . $binLogCurrent->getBinLogPosition() . ' bin log position' . PHP_EOL;

        // can be redis/nosql/file - something fast!
        // to speed up you can save every xxx time
        // you can also use signal handler for ctrl + c exiting script to wait for last event
        file_put_contents(self::getFileAndPath(), serialize($binLogCurrent));
    }

    /**
     * @param ConfigBuilder $builder
     * @return ConfigBuilder
     */
    public static function startFromPosition(ConfigBuilder $builder): ConfigBuilder
    {
        if (!is_file(self::getFileAndPath())) {
            return $builder;
        }
        /** @var BinLogCurrent $binLogCurrent */
        $binLogCurrent = unserialize(file_get_contents(self::getFileAndPath()));

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

    public static function run(array $config)
    {

    }

    private static function initConfigBuilder(array $config)
    {

    }
}