<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/16
 * Time: 19:19
 */

namespace LinLancer\PhpMySQLClickhouse\BinlogReader;


use Doctrine\Common\Cache\Cache;
use LinLancer\PhpMySQLClickhouse\Cache\DatabaseMappingRules;
use MySQLReplication\BinLog\BinLogCurrent;
use MySQLReplication\Config\ConfigBuilder;
use MySQLReplication\MySQLReplicationFactory;

class MySQLBinlogReader
{
    const CACHE_KEY = 'database:binlog:position';

    protected static $instance;

    protected static $rules;

    /**
     * @var Cache
     */
    private static $cache;

    private function __construct()
    {

    }

    private function __clone()
    {
        // TODO: Implement __clone() method.
    }

    private static function getInstance()
    {
        if (is_null(self::$instance))
            self::$instance = new self;
        return self::$instance;
    }


    public function setCache(Cache $cache)
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
    public function startFromPosition(ConfigBuilder $builder): ConfigBuilder
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

    public function stream(ConfigBuilder $config)
    {
        $binLogStream = new MySQLReplicationFactory(
            $this->startFromPosition($config)
                ->build()
        );
        $binLogStream->registerSubscriber(new EventsSubscriber);

        $binLogStream->run();
    }

    public static function run(array $config, Cache $cache)
    {
        $instance = self::getInstance();
        $instance->setCache($cache);
        $instance->updateTableCache($config, $cache);
        $configBuilder = $instance->initConfigBuilder($config);
        $instance->stream($configBuilder);
    }

    private function initConfigBuilder(array $config) :ConfigBuilder
    {
        $builder = new ConfigBuilder();
        $mysqlConfig = $config['mysql_database'];
        $builder = $builder->withUser($mysqlConfig['user'])
            ->withPort($mysqlConfig['port'])
            ->withPassword($mysqlConfig['password'])
            ->withHost($mysqlConfig['host'])
            ->withHeartbeatPeriod(2);
        $databases = array_map(function ($item) {
            $unit = explode('.', $item);
            return reset($unit);
        }, $mysqlConfig['tables']);
        $tables = array_map(function ($item) {
            $unit = explode('.', $item);
            return end($unit);
        }, $mysqlConfig['tables']);
        return $builder->withDatabasesOnly($databases)
        ->withTablesOnly($tables);
    }

    private function updateTableCache(array $config, Cache $cache)
    {
        self::$rules = new DatabaseMappingRules($config, $cache);
    }
}