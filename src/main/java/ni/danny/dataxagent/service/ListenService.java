package ni.danny.dataxagent.service;

import org.apache.curator.framework.CuratorFramework;

public interface ListenService {
    /**
     * 监控调度器状态
     */
    void watchDriver();

    void stopWatchDriver();

    /**
     * 调度器监控执行器状态[上线、下线]
     */
    void driverWatchExecutor();

    void stopDriverWatchExecutor();

    /**
     * 调度器监控任务执行器[任务执行被删除（完成、结束）]
     */
    void driverWatchJob();

    void stopDriverWatchJob();

    /**
     * 调度器监听KAFKA日志
     */
    void driverWatchKafkaMsg();

    void stopDriverWatchKafkaMsg();

    /**
     * 执行器监控自身任务[任务新增]
     */
    void executorWatchJobExecutor();



}
