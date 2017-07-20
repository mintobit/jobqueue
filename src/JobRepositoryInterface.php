<?php

namespace Mintobit\JobQueue;

interface JobRepositoryInterface
{
    /**
     * @param int   $typeId
     * @param array $data
     *
     * @return int
     */
    public function push($typeId, array $data);

    /**
     * @param int $typeId
     * @param int $workerId
     *
     * @return int|bool
     */
    public function pop($typeId, $workerId);

    /**
     * @param int $id
     *
     * @return void
     */
    public function done($id);

    /**
     * @param int $id
     *
     * @return void
     */
    public function delete($id);
}