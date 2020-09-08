package ni.danny.dataxagent.service;

import org.apache.curator.framework.CuratorFramework;

public interface ListenService {
    /**
     * 监控调度器状态
     */
    void watchDriver();

    /**
     * 调度器监控执行器状态
     */
    void driverWatchExecutor();

    /**
     * 执行器监控自身任务
     */
    void executorWatchJobExecutor();

    /**
     * 调度器监控任务执行器
     */
    void driverWatchJobExecutor();

    /**
     * 调度器监听KAFKA日志
     */
    void driverWatchKafkaMsg();

}
