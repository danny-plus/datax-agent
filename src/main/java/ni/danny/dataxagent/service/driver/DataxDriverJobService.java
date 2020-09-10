package ni.danny.dataxagent.service.driver;

import ni.danny.dataxagent.callback.DriverCallback;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.dto.event.DriverJobEventDTO;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import java.util.List;

/**
 * 由事件驱动
 * 事件：1.job/list/jobId [NODE_CREATED --新工作抵达，要进行拆分]；
 *      1.job/list/jobId [NODE_CHANGED --工作任务状态变更为拒绝，则删除此任务]；
 *      2.job/list/jobId/taskId [NODE_CREATED --新工作任务创建，要进行任务分配];
 *      2.job/list/jobId/taskId [NODE_CHANGED --工作任务状态变更为拒绝，检查任务拒绝数量，若超过限值，则拒绝整个JOB DATA为REJECT];
 *      2.job/list/jobId/taskId [NODE_CHANGED --工作任务状态变更为完成，检查任务完成数量，若=JOB子任务数，则变更整个JOB DATA为FINISH];
 *      3.job/list/jobId/taskId/ip:port-thread [NODE_CHANGED --任务执行状态变更为完成，则更新TASKID DATA为FINISH];
 *      3.job/list/jobId/taskId/ip:port-thread [NODE_CHANGED --任务执行状态变更为REJECT
 *                                              ，判断是否超过单个TASK拒绝次数，若超过，则变更TASKID DATA为REJEC，否则重新分配任务];
 *
 */
public interface DataxDriverJobService {

    /**
     * 任务巡检：扫描整个job/list目录
     *              1.维护一个全量的尚未执行完成的jobId-taskId的SET，用于快速分配任务
     *              2.维护一个全量的JOB SET,用于新到任务时，快速剔除已有任务，避免重复同时执行任务
     *         执行期间，暂停任务事件的重放
     *         将扫描到的未执行的TASK 维护到WAITFOREXECUTETASKSET中
     *         执行成功后，回调执行成功方法
     *         执行失败，回调失败方法
     */
    void scanJob(DriverCallback successCallback,DriverCallback failCallback);


    /**
     * 任务执行器事件分发器
     */
    void dispatchJobExecutorEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data);

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
     * TASK创建事件，尝试进行任务分配
     * @param eventDTO
     */
    void taskCreatedEvent(DriverJobEventDTO eventDTO);

    /**
     * TASK拒绝事件，检查相同JOB下的其他TASK状态，
     * 若超过限制数量，则更新JOB节点DATA为REJECT
     * 否则尝试进行任务分配
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
     * 否则尝试进行任务分配
     * @param eventDTO
     */
    void taskThreadRejectedEvent(DriverJobEventDTO eventDTO);


    /**
     * task的执行线程完成任务，更新TASK节点DATA为FINISH
     * @param eventDTO
     */
    void taskThreadFinishedEvent(DriverJobEventDTO eventDTO);

    /**
     * 尝试分配任务，通过从ideaThreadQueue中取出空闲执行线程，为其分配任务
     * 分配逻辑：1、将线程信息移除ideaThreadSET中，
     *         2、检查线程是否真的为空闲状态（/job/executor/ip:port/threadId）无子节点
     *         3、检查同JOB下的其他TASK是否存在执行中，并且在相同executor上，若存在且超过限值，则放弃此次任务分配，分配结果为失败
     *         3、创建/job/list/jobId/taskId/ip:port-threadId，DATA信息为INIT，失败则放弃本次分配，分配结果为失败
     *         4、创建/job/executor/ip:port/threadId/jobId-taskId,DATA为INIT，失败则删除上一步产生的节点，分配结果为失败
     *
     *         5、将线程放回ideaThreadSet
     * @param jobId
     * @param taskId
     * @return true 分配成功；false 分配失败
     */

    boolean dispatchTask(String jobId,String taskId);


}
