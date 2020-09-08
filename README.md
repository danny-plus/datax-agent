Zookeeper数据关联结构

-data-agent
    -driver[存储调度器信息]
    -executor
        -Ip:port[存储执行器信息]
    -job
        -list
            -jobId[存储任务JSON,原始]
                -taskId[存储任务JSON,拆分后]
                    -executorIp-threadId[存储任务执行状态][调度器负责：增加、删除；执行器负责：内容变更]
        -executorIp:port
            -threadId
                -jobId-taskId[存储任务追踪ID][调度器负责：增加、监控删除；执行器负责：删除、内容变更、监控增加]

