package ni.danny.dataxagent.service;

import org.apache.curator.framework.CuratorFramework;

public interface ListenService {
    /**
     * 监控调度器状态
     */
    void watchDriver();

    /**
     * 调度器监控执行器状态[上线、下线]
     */
    void driverWatchExecutor();

    /**
     * 调度器监控任务执行器[任务执行被删除（完成、结束）]
     */
    void driverWatchJobExecutor();

    /**
     * 执行器监控自身任务[任务新增]
     */
    void executorWatchJobExecutor();

    /**
     * 调度器监听KAFKA日志
     */
    void driverWatchKafkaMsg();

}
