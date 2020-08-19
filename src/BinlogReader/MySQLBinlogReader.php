<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/16
 * Time: 19:19
 */

namespace LinLancer\PhpMySQLClickhouse\BinlogReader;


use ClickHouseDB\Client;
use Doctrine\Common\Cache\Cache;
use LinLancer\PhpMySQLClickhouse\Cache\DatabaseMappingRules;
use LinLancer\PhpMySQLClickhouse\Clickhouse\ClickhouseClient;
use MySQLReplication\BinLog\BinLogCurrent;
use MySQLReplication\Config\ConfigBuilder;
use MySQLReplication\MySQLReplicationFactory;

class MySQLBinlogReader
{
    const CACHE_KEY = 'database:binlog:position';
    /**
     * @var self
     */
    protected static $instance;
    /**
     * @var DatabaseMappingRules
     */
    protected static $rules;
    /**
     * @var array
     */
    protected static $config;
    /**
     * @var Cache
     */
    private static $cache;

    /**
     * @var ClickhouseClient
     */
    private static $clickhouse;

    private function __construct()
    {
    }

    private function __clone()
    {
    }

    /**
     * @return MySQLBinlogReader
     */
    public static function getInstance()
    {
        if (is_null(self::$instance))
            self::$instance = new self;
        return self::$instance;
    }

    /**
     * @param Cache $cache
     */
    public function setCache(Cache $cache)
    {
        self::$cache = $cache;
    }

    /**
     * @return Cache
     */
    public function getCahce()
    {
        return self::$cache;
    }

    /**
     * @return DatabaseMappingRules
     */
    public function getTableRules()
    {
        return self::$rules;
    }

    /**
     * @return ClickhouseClient
     */
    public function getClickhouse()
    {
        return self::$clickhouse;
    }

    /**
     * @param BinLogCurrent $binLogCurrent
     */
    public static function save(BinLogCurrent $binLogCurrent): void
    {
//        echo 'saving file:' . $binLogCurrent->getBinFileName() . ', position:' . $binLogCurrent->getBinLogPosition() . ' bin log position' . PHP_EOL;

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

    /**
     * @param ConfigBuilder $config
     * @throws \Doctrine\DBAL\DBALException
     * @throws \MySQLReplication\BinLog\BinLogException
     * @throws \MySQLReplication\BinaryDataReader\BinaryDataReaderException
     * @throws \MySQLReplication\Config\ConfigException
     * @throws \MySQLReplication\Exception\MySQLReplicationException
     * @throws \MySQLReplication\Gtid\GtidException
     * @throws \MySQLReplication\JsonBinaryDecoder\JsonBinaryDecoderException
     * @throws \MySQLReplication\Socket\SocketException
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function stream(ConfigBuilder $config)
    {
        $binLogStream = new MySQLReplicationFactory(
            $this->startFromPosition($config)
                ->build()
        );
        $binLogStream->registerSubscriber(new EventsSubscriber);

        $binLogStream->run();
    }

    /**
     * @param array $config
     * @param Cache $cache
     * @throws \Doctrine\DBAL\DBALException
     * @throws \MySQLReplication\BinLog\BinLogException
     * @throws \MySQLReplication\BinaryDataReader\BinaryDataReaderException
     * @throws \MySQLReplication\Config\ConfigException
     * @throws \MySQLReplication\Exception\MySQLReplicationException
     * @throws \MySQLReplication\Gtid\GtidException
     * @throws \MySQLReplication\JsonBinaryDecoder\JsonBinaryDecoderException
     * @throws \MySQLReplication\Socket\SocketException
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public static function run(array $config, Cache $cache)
    {
        self::$config = $config;
        $instance = self::getInstance();
        $instance->setCache($cache);
        $instance->initClickhouseClient($config);
        $instance->updateTableCache();
        $configBuilder = $instance->initConfigBuilder($config);
        $instance->stream($configBuilder);
    }

    /**
     * @param array $config
     * @return ConfigBuilder
     */
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

    /**
     * @param $config
     */
    private function initClickhouseClient($config)
    {
        $clickhouseConfig = $config['clickhouse_database'];
        $config = [
            'host' => $clickhouseConfig['host'],
            'port' => $clickhouseConfig['port'],
            'username' => $clickhouseConfig['user'],
            'password' => $clickhouseConfig['password']
        ];

        self::$clickhouse = new ClickhouseClient($config);
    }

    /**
     * 更新表结构缓存
     */
    public function updateTableCache()
    {
        self::$rules = new DatabaseMappingRules(self::$config, self::$cache, self::$clickhouse);
    }
}