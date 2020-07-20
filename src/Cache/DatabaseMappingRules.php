<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/18
 * Time: 15:56
 */

namespace LinLancer\PhpMySQLClickhouse\Cache;

use Doctrine\Common\Cache\PredisCache;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Column;
use function foo\func;
use LinLancer\PhpMySQLClickhouse\Clickhouse\ClickhouseClient;
use Predis\Client;

class DatabaseMappingRules
{
    const MYSQL_CACHE_KEY = 'database:mysql:table:';

    const CLICKHOUSE_CACHE_KEY = 'database:clickhouse:table:';

    const MAPPING_CACHE_KEY = 'database:table:mapping';

    protected $config;

    protected $cache;

    public function __construct($config)
    {
        $this->config = $config;
        $client = new Client($config['redis']);
        $this->cache = new PredisCache($client);
        $this->initMySQLDatabases();
        $mappingRules = $this->initMapping();
        $this->cache->save(self::MYSQL_CACHE_KEY, json_encode($mappingRules));
        $this->initClickhouseDatabase($mappingRules);
    }

    /**
     * @return array
     */
    public function initMapping()
    {
        if (!isset($this->config['table_mapping']) || empty($this->config['table_mapping']))
            $tableMapping = [];
        else
            $tableMapping = $this->config['table_mapping'];

        $tableGroup = array_column($this->config['mysql_database'], 'tables');
        $mappingRules = array_map(function ($tables) {
            $mapping = [];
            foreach ($tables as $table) {
                $mapping[$table] = $table;
            }
            return $mapping;
        }, $tableGroup);
        $mappingRules = array_merge(...$mappingRules);

        return array_merge($mappingRules, $tableMapping);
    }

    /**
     * @param $mappingRules
     * @throws \Exception
     */
    public function initClickhouseDatabase($mappingRules)
    {
        $clickhouseTables = array_values($mappingRules);
        $clickhouseConfig = $this->config['clickhouse_database'];
        $config = [
            'host' => $clickhouseConfig['host'],
            'port' => $clickhouseConfig['port'],
            'username' => $clickhouseConfig['user'],
            'password' => $clickhouseConfig['password']
        ];
        $clickhouseClient = new ClickhouseClient($config);
        $this->checkClickhouseTables($clickhouseClient, $clickhouseTables);
    }

    /**
     * @param ClickhouseClient $client
     * @param                  $tables
     * @throws \Exception
     */
    public function checkClickhouseTables(ClickhouseClient $client, $tables)
    {
        foreach ($tables as $table) {
            $tableArr = explode('.', $table);
            if (count($tableArr) !== 2)
                throw new \Exception('Table name pattern is illegal, it must be like \'database.table\'');
            $db = reset($tableArr);
            $tableName = end($tableArr);

            $bool = $client->isExist($db, $tableName);

            if ($bool === false) {
//                $client->createDatabase($db);
                throw new \Exception(sprintf('There is no table named %s found in clickhouse database %s', $tableName, $db));
            }

        }
    }

    /**
     * @throws \Doctrine\DBAL\DBALException
     */
    public function initMySQLDatabases()
    {
        foreach ($this->config['mysql_database'] as $config) {
            $config['driver'] = 'pdo_mysql';
            $config['dbname'] = 'information_schema';
            $conn = DriverManager::getConnection($config);
            $sm = $conn->getSchemaManager();
            $databases = $sm->listDatabases();
            $tables = $config['tables'];
            $schemas = array_map(function ($table) {
                $tableArr = explode('.', $table);
                return reset($tableArr);
            }, $tables);
            if (!empty($check = array_diff($schemas, $databases))) {
                throw new \Exception(sprintf('Database %s not found on connection %s@%s', implode(' ', $check), $config['user'], $config['host']));
            }
            $this->checkMySQLTables($sm, $tables);

        }
    }

    /**
     * @param AbstractSchemaManager $sm
     * @param                       $tables
     * @throws \Exception
     */
    public function checkMySQLTables(AbstractSchemaManager $sm, $tables)
    {
        foreach ($tables as $table) {
            $tableArr = explode('.', $table);
            if (count($tableArr) !== 2)
                throw new \Exception('Table name pattern is illegal, it must be like \'database.table\'');
            $db = reset($tableArr);
            $tableName = end($tableArr);

            /**
             *@var Column[] $tableColumns
             */
            $tableColumns = $sm->listTableColumns($tableName, $db);

            if (empty($tableColumns))
                throw new \Exception(sprintf('Table %s is not found in database %s', $tableName, $db));

            $this->cache->save(self::MYSQL_CACHE_KEY.$tableName, serialize($tableColumns));
        }
    }
}