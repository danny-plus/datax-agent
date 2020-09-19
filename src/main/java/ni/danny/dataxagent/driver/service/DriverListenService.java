package ni.danny.dataxagent.driver.service;


public interface DriverListenService {
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



}
