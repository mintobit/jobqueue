<?php

namespace Mintobit\JobQueue;

use \PDO as Connection;

final class JobRepository implements JobRepositoryInterface
{
    /**
     * @var Connection
     */
    private $connection;
    /**
     * @var
     */
    private $table;

    /**
     * @param Connection $connection
     * @param string     $table
     */
    public function __construct(Connection $connection, $table)
    {
        $this->connection = $connection;
        $this->table = $table;
    }

    /**
     * @throws \PDOException
     */
    public function push($typeId, array $data)
    {
        $query = sprintf('
            INSERT INTO %s (type_id, status_id, worker_id, data, created, updated)
            VALUES (:type_id, :new_status_id, 0, :data, :created, :updated)
        ', $this->table);
        $dateTime = gmdate('Y-m-d H:i:s');

        $preparedStatement = $this->connection->prepare($query);
        $preparedStatement->execute(array(
            ':type_id'          => $typeId,
            ':new_status_id'    => JobStatuses::SNEW,
            ':data'             => serialize($data),
            ':created'          => $dateTime,
            ':updated'          => $dateTime
        ));

        return $this->connection->lastInsertId();
    }

    /**
     * @throws \PDOException
     */
    public function pop($typeId, $workerId)
    {
        if ($startedJobId = $this->started($typeId, $workerId)) {
            return $startedJobId;
        }

        $query = sprintf('
            UPDATE %s
            SET status_id = :started_status_id,
            worker_id = :worker_id,
            updated = :updated
            WHERE type_id = :type_id
            AND status_id = :new_status_id
            AND worker_id = 0
            ORDER BY created ASC
            LIMIT 1
        ', $this->table);

        $preparedStatement = $this->connection->prepare($query);
        $preparedStatement->execute(array(
            ':started_status_id'    => JobStatuses::STARTED,
            ':worker_id'            => $workerId,
            ':updated'              => gmdate('Y-m-d H:i:s'),
            ':type_id'              => $typeId,
            ':new_status_id'        => JobStatuses::SNEW,
        ));

        return $this->started($typeId, $workerId);
    }

    /**
     * @throws \PDOException
     */
    public function done($id)
    {
        $query = sprintf('
            UPDATE %s
            SET status_id = :finished_status_id
            WHERE id = :id
            AND status_id = :started_status_id
        ', $this->table);

        $preparedStatement = $this->connection->prepare($query);
        $preparedStatement->execute(array(
            ':finished_status_id'   => JobStatuses::FINISHED,
            ':id'                   => $id,
            ':started_status_id'    => JobStatuses::STARTED
        ));
    }

    /**
     * @throws \PDOException
     */
    public function delete($id)
    {
        $query = sprintf('
            DELETE
            FROM %s
            WHERE id = :id
            AND status_id = :finished_status_id
        ', $this->table);

        $preparedStatement = $this->connection->prepare($query);
        $preparedStatement->execute(array(
            ':id'                   => $id,
            ':finished_status_id'   => JobStatuses::FINISHED
        ));
    }

    /**
     * Check if there is already started job
     *
     * @param int $typeId
     * @param int $workerId
     *
     * @return bool|int
     */
    private function started($typeId, $workerId)
    {
        $query = sprintf('
            SELECT id
            FROM %s
            WHERE type_id = :type_id
            AND status_id = :started_status_id
            AND worker_id = :worker_id
            LIMIT 1
        ', $this->table);

        $preparedStatement = $this->connection->prepare($query);
        $preparedStatement->execute(array(
            ':type_id'           => $typeId,
            ':started_status_id' => JobStatuses::STARTED,
            ':worker_id'         => $workerId
        ));

        $result = $preparedStatement->fetchColumn();

        return $result === false
            ? $result
            : (int) $result;
    }
}