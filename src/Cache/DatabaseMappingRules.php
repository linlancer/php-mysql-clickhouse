<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/18
 * Time: 15:56
 */

namespace LinLancer\PhpMySQLClickhouse\Cache;

use Doctrine\Common\Cache\Cache;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Table;
use LinLancer\PhpMySQLClickhouse\Clickhouse\ClickhouseClient;
use LinLancer\PhpMySQLClickhouse\Clickhouse\TableDefinitionParser;

class DatabaseMappingRules
{
    const MYSQL_CACHE_KEY = 'database:mysql:table:';

    const CLICKHOUSE_CACHE_KEY = 'database:clickhouse:table:';

    const MAPPING_CACHE_KEY = 'database:table:mapping';

    protected $config;

    protected $cache;

    protected $client;

    protected $mapping = [];

    public function __construct($config, Cache $cache, ClickhouseClient $client)
    {
        $this->config = $config;
        $this->cache = $cache;
        $this->client = $client;
        $this->initMySQLDatabases();
        $this->mapping = $this->initMapping();
        $this->cache->save(self::MAPPING_CACHE_KEY, json_encode($this->mapping));
        $this->initClickhouseDatabase($this->mapping);
    }

    /**
     * @param $schema
     * @param $table
     * @return Table
     */
    public function getMysqlTable($schema, $table)
    {
        $table = $this->cache->fetch(self::MYSQL_CACHE_KEY . $schema . ':' . $table);
        return unserialize($table);
    }

    /**
     * @param $schema
     * @param $table
     * @return TableDefinitionParser
     */
    public function getClickhouseTable($schema, $table)
    {
        $table = $this->cache->fetch(self::CLICKHOUSE_CACHE_KEY . $schema . ':' . $table);
        $obj = unserialize($table);
        if ($obj === false)
            throw new \Exception('Couldn\'t found cache key :' . self::CLICKHOUSE_CACHE_KEY . $schema . ':' . $table);
        return $obj;
    }

    /**
     * @return array
     */
    public function getMapingRules()
    {
        $rules = $this->cache->fetch(self::MAPPING_CACHE_KEY);
        return json_decode($rules, true);
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

        $tableGroup = $this->config['mysql_database']['tables'];
        $mappingRules = [];
        foreach ($tableGroup as $table) {
            $mappingRules[$table] = $table;
        }
        return array_merge($mappingRules, $tableMapping);
    }

    /**
     * @param $mappingRules
     * @throws \Exception
     */
    public function initClickhouseDatabase($mappingRules)
    {
        $mysqlDatabaseConfig = $this->config['mysql_database'];
        $sourceConfig = [
            'host' => $mysqlDatabaseConfig['host'],
            'port' => $mysqlDatabaseConfig['port'],
            'username' => $mysqlDatabaseConfig['user'],
            'password' => $mysqlDatabaseConfig['password']
        ];
        $clickhouseClient = $this->client;
        $this->checkClickhouseTables($clickhouseClient, $mappingRules, $sourceConfig);
    }

    /**
     * @param ClickhouseClient $client
     * @param                  $tables
     * @param                  $sourceConfig
     * @throws \Exception
     */
    public function checkClickhouseTables(ClickhouseClient $client, $tables, $sourceConfig)
    {
        list ($host, $port, $user, $passwd) = array_values($sourceConfig);
        foreach ($tables as $origin =>  $table) {
            if (class_exists($table))
                throw new \Exception('couldn\'t be supported yet');
            $originArr = explode('.', $origin);
            $tableArr = explode('.', $table);
            if (count($tableArr) !== 2 && count($originArr) !== 2)
                throw new \Exception('Table name pattern is illegal, it must be like \'database.table\'');
            $db = reset($tableArr);
            $tableName = end($tableArr);

            $originalDb = reset($originArr);
            $originalTableName = end($originArr);
            /**
             * @var Table $originTableDetail
             */
            $originTableDetail = $this->getOriginTableDetails($originalDb, $originalTableName);

            $primaryColumns = $originTableDetail->getPrimaryKey()->getColumns();
            $primaryKey = reset($primaryColumns);

            $bool = $client->isExist($db, $tableName);

            if ($bool === false) {
                $client->createDatabase($db);
//                throw new \Exception(sprintf('There is no table named %s found in clickhouse database %s', $tableName, $db));
                $sql = <<<CREATE_TABLE_SQL
                    CREATE TABLE IF NOT EXISTS $db.$tableName
                    ENGINE = MergeTree
                    ORDER BY $primaryKey AS
                    SELECT *
                    FROM mysql('$host:$port', '$originalDb', '$originalTableName', '$user', '$passwd');
CREATE_TABLE_SQL;

                $client->query($sql);
            }
            $clickhouseTable = $client->getTableDefinition($db, $tableName);
            $this->cache->save(self::CLICKHOUSE_CACHE_KEY . $db . ':' . $tableName, serialize($clickhouseTable));

        }
    }

    /**
     * @throws \Doctrine\DBAL\DBALException
     */
    public function initMySQLDatabases()
    {
        $config = $this->config['mysql_database'];
        $config['driver'] = 'pdo_mysql';
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
        $this->checkMySQLTables($conn, $sm, $tables);
    }

    /**
     * @param AbstractSchemaManager $sm
     * @param                       $tables
     * @throws \Exception
     */
    public function checkMySQLTables(Connection $conn, AbstractSchemaManager $sm, $tables)
    {
        foreach ($tables as $table) {

            $tableArr = explode('.', $table);
            if (count($tableArr) !== 2)
                throw new \Exception('Table name pattern is illegal, it must be like \'database.table\'');
            $db = reset($tableArr);
            $tableName = end($tableArr);

            $conn->exec(sprintf('use %s;', $db));
            $tableDetails = $sm->listTableDetails($tableName);

            if (empty($tableDetails->getColumns()))
                throw new \Exception(sprintf('Table %s is not found in database %s', $tableName, $db));

            $this->cache->save(self::MYSQL_CACHE_KEY . $db . ':' . $tableName, serialize($tableDetails));

        }
    }

    private function getOriginTableDetails($db, $table)
    {
        return unserialize($this->cache->fetch(self::MYSQL_CACHE_KEY . $db . ':' . $table));
    }
}