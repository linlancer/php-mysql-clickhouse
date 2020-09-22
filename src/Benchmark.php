<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/9/22
 * Time: 17:59
 */

namespace LinLancer\PhpMySQLClickhouse;

/**
 * Class Benchmark
 * @package LinLancer\PhpMySQLClickhouse
 * @method static start($arg)
 * @method static end($arg)
 * @method static process($arg)
 * @method static exception($arg)
 * @method static log($arg)
 */
class Benchmark
{
    protected static $instance;

    protected static $startMicro;

    protected static $endMicro;

    protected static $allowFunc = [
        'start',
        'end',
        'process',
        'exception',
        'log',
    ];

    public static function getInstance()
    {
        if (static::$instance === null) {
            return new static;
        }
        return self::$instance;
    }

    public static function __callStatic($name, $arguments)
    {
        if (in_array($name, self::$allowFunc)) {
            $instance = self::getInstance();
            switch ($name) {
                case 'start':
                    $instance->handleStart(...$arguments);
                    break;
                case 'end':
                    $instance->handleEnd(...$arguments);
                    break;
                case 'process':
                    $instance->handleProcess(...$arguments);
                    break;
                case 'exception':
                    $instance->handleException(...$arguments);
                    break;
                case 'log':
                    $instance->handleLog(...$arguments);
                    break;
                default:
                    break;
            }
        } else {
            exit('Invalid Function Name');
        }
    }

    public function handleStart($arg)
    {
        static::$startMicro = microtime(true);
        $message = '【START】 ' . strval($arg);
        static::line($message);
    }

    public function handleEnd($arg)
    {
        static::$endMicro = microtime(true);
        $message = '【END】 ' . strval($arg) . static::micro();
        static::line($message);
    }

    public function handleProcess($arg)
    {
        static::$endMicro = microtime(true);
        $message = '【HANDING】 ' . strval($arg);
        static::line($message);
    }

    public function handleException($arg)
    {
        static::$endMicro = microtime(true);
        $message = '【EXCEPTION】 ' . strval($arg);
        static::line($message);
    }

    public function handleLog($arg)
    {
        static::$endMicro = microtime(true);
        $message = '【RECORD】 ' . strval($arg);
        static::line($message);
    }

    private static function date()
    {
        return sprintf('【%s】', date('Y-m-d H:i:s'));
    }

    private static function logger()
    {

    }

    private static function micro()
    {
        $duration = intval((static::$endMicro - static::$startMicro) * 1000);
        echo sprintf('【耗时】%s ms.', $duration).PHP_EOL;
    }
    private static function line($content)
    {
        echo static::date() . ' ' . $content.PHP_EOL;
    }
}