<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/9/22
 * Time: 20:50
 */

namespace LinLancer\PhpMySQLClickhouse\Task;


class RedisQueue
{
    const CACHE_PREFIX = 'redis:queue:';

    protected $redis;

    public function __construct(array $config)
    {
        $this->redis = new \Redis;
        $this->redis->connect($config['host'], $config['port']);
        $this->redis->auth($config['password']);
        $this->redis->select($config['database']);

    }

    public function push($queueName, string $data)
    {
        $this->redis->lPush($queueName, $data);
    }

    public function pop($queueName)
    {
        return $this->redis->rPop($queueName);
    }
}