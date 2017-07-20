<?php

namespace Mintobit\JobQueue;

interface JobConsumerInterface
{
    /**
     * @param int $jobId
     *
     * @return void
     */
    public function consume($jobId);
}