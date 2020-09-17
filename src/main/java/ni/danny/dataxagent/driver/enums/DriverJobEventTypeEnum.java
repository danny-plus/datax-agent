package ni.danny.dataxagent.driver.enums;
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
 *
 *      *** waitTaskSet 只由scanJob,dispatchTask 管理
 *
 */
public enum DriverJobEventTypeEnum {
    JOB_SCAN,
    JOB_CREATED,
    JOB_REJECTED,
    JOB_FINISHED,
    TASK_CREATED,
    TASK_REJECTED,
    TASK_FINISHED,
    TASK_THREAD_FINISHED,
    TASK_THREAD_REJECTED;
}
