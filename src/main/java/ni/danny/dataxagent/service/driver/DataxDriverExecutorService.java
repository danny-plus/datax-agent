package ni.danny.dataxagent.service.driver;

import ni.danny.dataxagent.callback.DriverCallback;
import ni.danny.dataxagent.dto.event.DriverJobEventDTO;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

/**
 *
 * 事件驱动：1.executor/Ip:Port [NODE_CREATED --执行器上线，根据最大线程数创建THREAD]
 *         2.executor/Ip:Port [NODE_REMOVED --执行器下线，尝试回收所有资源]
 *         3.job/executor/Ip:Port/thead [NODE_CREATED --执行器工作线程被创建，分配任务]
 *         4.job/executor/Ip:Port/thread [NODE_CHANGED --NEW DATA=WAITRECYCLE，标记为等待回收，将不再进入idleThreadSet中 ]
 *         5.job/executor/Ip:Port/thread [NODE_CHANGED --NEW DATA=READY，标记为已准备完成，若节点本身无执行任务则
 *                                          ，尝试进行工作分配，分配失败则进入idleThreadSet中]
 *         6.job/executor/Ip:Port/thread [NODE_REMOVED --执行器工作线程被删除，若该executor下所有thread都被删除
 *                                              ，则删除此job/executor/ip:port节点]
 *         7.job/executor/Ip:Port/thread/jobId-taskId [NODE_CREATED --执行器工作线程被分配了任务]
 *         8.job/executor/Ip:Port/thread/jobId-taskId [NODE_CHANGED --NEW DATA 为traceId]
 *         8.job/executor/Ip:Port/thread/jobId-taskId [NODE_REMOVED --任务已完成 --执行器工作线程移除了任务
 *                                              ，若线程标记为待回收，则直接删除节点，否则重新分配任务，没有新任务则加入空闲线程]
 *         9.job/executor/Ip:Port/thread/jobId-taskId [NODE_REMOVED --任务被拒绝 --执行器工作线程移除了任务
 *                                                  ，若线程标记为待回收，则直接删除节点，重新分配任务，没有新任务则加入空闲线程]
 *
 */
public interface DataxDriverExecutorService {
    /**
     * 执行器巡检
     * 停止执行器事件推送
     * 扫描所有在线执行器 /executor，放入一个临时SET中
     * 扫描所有job/executor
     * 逐个扫描job/executor/ip:port/thread，
     * ----若该thread所在对executor不是在线，则检查该thread是否有关联任务，若存在关联任务则标记为WAITRECYCLE，否则直接删除该节点
     * ----若该thread所在的executor在线，则检查thread是否为READY，若不READY则变更为READY，并加入idleThreadSet中
     *
     * 恢复执行器事件推送
     */
    void scanExecutor(DriverCallback successCallback,DriverCallback failCallback);

    /**
     * 执行器事件分发器
     */
    void dispatchExecutorEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data);

    /**
     * 执行器节点创建事件【上线】
     * 根据最大线程数创建 /job/executor/ip:port/thread 节点，节点数据为 READY
     * 若thread节点已存在，则不进行创建，若THREAD节点DATA = WAITRECYCLE 待回收，则重新更新为READY状态
     * @param eventDTO
     */
    void executorCreatedEvent(DriverJobEventDTO eventDTO);

    /**
     * 执行器节点删除事件【下线】
     * 扫描/job/executor/ip:port 所有thread
     * 若thread存在关联任务则更新thread DATA=WAITRECYCLE,标记为等待删除
     * 若thread无关联任务，则删除thread节点
     * @param eventDTO
     */
    void executorRemovedEvent(DriverJobEventDTO eventDTO);


    /**
     * 工作线程被创建事件
     * 尝试进行任务分配
     *
     * @param eventDTO
     */
    void threadCreatedEvent(DriverJobEventDTO eventDTO);

    /**
     * 工作线程被标记为待回收
     * 检查关联任务状态，/job/list/jobId/taskId/ 状态是否为FINISH OR REJECT
     * 若是以上两种状态，则移除本线程
     * @param eventDTO
     */
    void threadUpdateWaitRecycleEvent(DriverJobEventDTO eventDTO);

    /**
     * 工作线程被标记为READY事件
     * 尝试分配TASK
     * @param eventDTO
     */
    void threadUpdateReadyEvent(DriverJobEventDTO eventDTO);


    /**
     * 工作线程被删除事件，执行器下线导致的线程回收
     * 检查同一一个executor下，无其他执行器，则移除/job/executor/ip:port
     * @param eventDTO
     */
    void threadRemovedEvent(DriverJobEventDTO eventDTO);

    /**
     * 工作线程被关联新任务【此为执行器逻辑什么都不用做】
     * @param eventDTO
     */
    void threadTaskCreatedEvent(DriverJobEventDTO eventDTO);



    /**
     * /job/executor/ip:port/thread/jobId-taskId
     * 工作线程-TASK节点，内容变更为traceId
     * @param eventDTO
     */
    void threadTaskUpdatedEvent(DriverJobEventDTO eventDTO);


    /**
     * /job/executor/ip:port/thread/jobId-taskId
     * 工作线程-TASK节点被删除【FINISH/REJECT】
     *  --任务已完成 --执行器工作线程移除了任务，若线程标记为待回收，则直接删除节点，否则重新分配任务，没有新任务则加入空闲线程]
     *  --任务被拒绝 --执行器工作线程移除了任务，若线程标记为待回收，则直接删除节点，重新分配任务，没有新任务则加入空闲线程]
     * @param eventDTO
     */
    void threadTaskRemovedEvent(DriverJobEventDTO eventDTO);


    /**
     * 尝试分配任务
     * 1.从waitforexecuteTaskQueue取出一个TASK
     * 2.检查任务具体状态 job/list/jobId/taskId ,并非REJECT OR FINISH
     * 3.检查任务是否曾关联该任务，如果关联过，则放弃本次任务分配，分配结果为失败
     * 4.创建/job/list/jobId/taskId/executor-thread,DATA=INIT，失败则放弃本次任务分配，分配结果失败
     * 5.创建/job/executor/ip:port/thread/jobId-taskId,DATA=INIT,失败则放弃本次任务分配，分配结果失败
     * 任务分配失败时，将thread放回idleThreadQueue中
     * @param executor
     * @param thread
     * @return true 分配成功，false分配失败
     */
    boolean dispatchTask(String executor,String thread);


}
