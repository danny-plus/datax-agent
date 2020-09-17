package ni.danny.dataxagent.driver.service;

import ni.danny.dataxagent.callback.DriverCallback;
import ni.danny.dataxagent.driver.dto.event.DriverJobEventDTO;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

/**
 * 由事件驱动
 * 事件：1.job/list/jobId [NODE_CREATED --新工作抵达，要进行拆分]；
 *      1.job/list/jobId [NODE_CHANGED --工作任务状态变更为拒绝，则删除此任务]；
 *      2.job/list/jobId/taskId [NODE_CREATED --新工作任务创建，要进行任务分配];
 *      2.job/list/jobId/taskId [NODE_CHANGED --工作任务状态变更为拒绝，检查任务拒绝数量，若超过限值，则拒绝整个JOB DATA为REJECT];
 *      2.job/list/jobId/taskId [NODE_CHANGED --工作任务状态变更为完成，检查任务完成数量，若=JOB子任务数，则变更整个JOB DATA为FINISH];
 *      3.job/list/jobId/taskId/ip:port-thread [NODE_CHANGED --任务执行状态变更为完成，则更新TASKID DATA为FINISH];
 *      3.job/list/jobId/taskId/ip:port-thread [NODE_CHANGED --任务执行状态变更为REJECT
 *                                              ，判断是否超过单个TASK拒绝次数，若超过，则变更TASKID DATA为REJECT,否则重新分配任务];
 *
 *
 *      *** waitForExecuteTaskQueue 只由scanJob,和taskcreated,负责添加
 *                                  由DISPATCHTASK,负责移除，移除使用POLL方法
 *
 */
public interface DataxDriverJobService {

    /**
     * 任务巡检
     *      暂停任务事件推送【DelayQueue暂停】-只收集，不推送
     *      清空waitTaskSet
     *      扫描所有jobId/taskId
     *      将无关联执行线程【执行中】的加入临时set
     *      waitTaskSet.addAll(tmpSet)
     *      启动任务事件推送
     *
     */
    void scanJob();


    /**
     * 任务执行器事件分发器
     */
    void dispatchJobEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data);

    /**
     * JOB创建事件，进行任务拆分
     */
    void jobCreatedEvent(DriverJobEventDTO eventDTO);

    /**
     * JOB拒绝事件，进行告警或记录
     */
    void jobRejectedEvent(DriverJobEventDTO eventDTO);

    /**
     * JOB完成事件，进行记录，并删除JOB
     * @param eventDTO
     */
    void jobFinishedEvent(DriverJobEventDTO eventDTO);



    /**
     * TASK创建事件，尝试分配任务，此事件不由ZOOKEEPER分发得到
     * @param eventDTO
     */
    void taskCreatedEvent(DriverJobEventDTO eventDTO);


    /**
     * TASK拒绝事件，检查相同JOB下的其他TASK状态，
     * 若超过限制数量，则更新JOB节点DATA为REJECT
     *
     * @param eventDTO
     */
    void taskRejectedEvent(DriverJobEventDTO eventDTO);


    /**
     * TASK完成事件，检查相同JOB下其他的TASK状态，若都为完成，则更新JOB节点DATA为FINISH
     * @param eventDTO
     */
    void taskFinishedEvent(DriverJobEventDTO eventDTO);

    /**
     * task的执行线程拒绝任务，检查相同TASK下的其他执行线程记录，
     * 是否全部为拒绝，且超过限制数量，若超过，则更新TASK节点DATA为REJECT
     * 否则尝试添加进waitForExecuteTaskQueue
     *      * 调用任务分配
     * @param eventDTO
     */
    void taskThreadRejectedEvent(DriverJobEventDTO eventDTO);


    /**
     * task的执行线程完成任务，更新TASK节点DATA为FINISH
     * @param eventDTO
     */
    void taskThreadFinishedEvent(DriverJobEventDTO eventDTO);


    void dispatchEvent(DriverJobEventDTO dto);

}
