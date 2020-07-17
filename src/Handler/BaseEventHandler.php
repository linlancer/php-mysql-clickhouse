<?php
/**
 * Created by PhpStorm.
 * User: $_s
 * Date: 2020/7/17
 * Time: 11:19
 */

namespace LinLancer\PhpMySQLClickhouse\Handler;


use MySQLReplication\Event\DTO\EventDTO;
use MySQLReplication\Event\DTO\RowsDTO;

abstract class BaseEventHandler
{
    protected $db;

    protected $table;

    /**
     * @var EventDTO
     */
    protected $event;

    public function __construct(EventDTO $event)
    {
        $this->event = $event;
        $this->initEvent();
    }

    public function initEvent()
    {
        if ($this->event instanceof RowsDTO) {
            $this->db = $this->event->getTableMap()->getDatabase();
            $this->table = $this->event->getTableMap()->getTable();
        }

        echo $this->event;
    }

    public abstract function handle();

}