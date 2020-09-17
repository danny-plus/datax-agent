package ni.danny.dataxagent.driver.service;

import ni.danny.dataxagent.callback.DriverCallback;
import ni.danny.dataxagent.driver.dto.event.DriverExecutorEventDTO;
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
 *
 *        **** idleThreadSet仅由ScanExecutor , executorRemovedEvent ,dispatchTask 方法管理
 */
public interface DataxDriverExecutorService {
    /**
     * 执行器巡检
     * 停止执行器事件推送【】-只收集，不推送
     * 清空idleThreadSet
     * 扫描所有在线executor
     * 根据在线executor扫描得到所有thread,并筛选出无任务的thread
     * 加入临时SET
     * idleThreadSet.addAll(tempSet)
     * 恢复执行器事件推送
     */
    void scanExecutor();

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
    void executorCreatedEvent(DriverExecutorEventDTO eventDTO);

    /**
     * 执行器节点删除事件【下线】
     * 扫描/job/executor/ip:port 所有thread
     * 若thread存在关联任务则更新thread DATA=WAITRECYCLE,标记为等待删除
     * 若thread无关联任务，则删除thread节点
     * @param eventDTO
     */
    void executorRemovedEvent(DriverExecutorEventDTO eventDTO);


    /**
     * 工作线程被创建事件
     * 尝试进行任务分配
     *
     * @param eventDTO
     */
    void threadCreatedEvent(DriverExecutorEventDTO eventDTO);

    /**
     * 工作线程被标记为待回收
     * 检查关联任务状态，/job/list/jobId/taskId/ 状态是否为FINISH OR REJECT
     * 若是以上两种状态，则移除本线程
     * @param eventDTO
     */
    void threadUpdateWaitRecycleEvent(DriverExecutorEventDTO eventDTO);

    /**
     * 工作线程被标记为READY事件,检查是否有正在执行的任务,没有关联任务则尝试分配任务
     * 尝试分配TASK
     * @param eventDTO
     */
    void threadUpdateReadyEvent(DriverExecutorEventDTO eventDTO);


    /**
     * 工作线程被删除事件，执行器下线导致的线程回收
     * 检查同一一个executor下，无其他执行器，则移除/job/executor/ip:port
     * @param eventDTO
     */
    void threadRemovedEvent(DriverExecutorEventDTO eventDTO);

    /**
     * 工作线程被关联新任务【此为执行器逻辑什么都不用做】
     * @param eventDTO
     */
    void threadTaskCreatedEvent(DriverExecutorEventDTO eventDTO);



    /**
     * /job/executor/ip:port/thread/jobId-taskId
     * 工作线程-TASK节点，内容变更为traceId
     * @param eventDTO
     */
    void threadTaskUpdatedEvent(DriverExecutorEventDTO eventDTO);


    /**
     * /job/executor/ip:port/thread/jobId-taskId
     * 工作线程-TASK节点被删除【FINISH/REJECT】
     *  --任务已完成 --执行器工作线程移除了任务，若线程标记为待回收，则直接删除节点，否则重新分配任务，没有新任务则加入空闲线程]
     *  --任务被拒绝 --执行器工作线程移除了任务，若线程标记为待回收，则直接删除节点，重新分配任务，没有新任务则加入空闲线程]
     * @param eventDTO
     */
    void threadTaskRemovedEvent(DriverExecutorEventDTO eventDTO);


    void dispatchEvent(DriverExecutorEventDTO eventDTO);



}
