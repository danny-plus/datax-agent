package ni.danny.dataxagent.driver.service;

import ni.danny.dataxagent.driver.dto.ExecutorThreadDTO;
import ni.danny.dataxagent.driver.dto.JobTaskDTO;
import ni.danny.dataxagent.driver.dto.event.DriverEventDTO;
import ni.danny.dataxagent.driver.dto.event.DriverExecutorEventDTO;
import ni.danny.dataxagent.driver.dto.event.DriverJobEventDTO;
import ni.danny.dataxagent.dto.DataxDTO;

public interface DataxDriverService {
    /**
     * 注册成为Driver【尝试创建Driver节点，并写入自身信息IP:PORT】
     * 注册成功：本地标记当前Driver信息，标记Driver进入初始化状态，
     *         创建两个延时队列【执行器事件队列，任务事件队列】
     *         并启动事件监听【执行器节点上、下线事件；执行器任务节点完成、拒绝事件；新工作创建事件】，调用初始化方法
     *
     * 注册失败：本地标记当前Driver信息，并监听当前的Driver，若Driver被删除【临时节点，SESSION断开则会被删除】，重试本方法
     *
     */
    void regist();

    /**
     * 启动事件监听
     * 监听：1.监控/executor[执行器根目录],监控所有执行器上线、下线事件，将事件写入执行器事件队列
     *      2.监控/job/executor[job执行器目录],监控 /job/executor/thread/jobId-taskId[某个子任务的移除事件
     *          ,子任务被移除代表子任务完成或拒绝]，将事件写入任务事件队列【使用disruptor队列】
     *      3.监控/job/list[job根目录]，监控/job/list/jobId[新增事件，代表有新到数据同步工作]，将此事件写入任务事件队列【使用disruptor队列】
     *      4.监听KAFKA-datax-log队列消息，维护可获取到的TASK信息：状态包括「INIT,START,RUNNING,FINISH,REJECT」,状态变化根据ENUM信息管控
     *
     */
    void listen();

    void stopListen();

    /**
     * 成为Driver后，进行初始化
     *      启动执行器巡检
     *      启动工作巡检
     */
    void init();

    /**
     * 检查自身是否是当前的DRIVER
     * @return
     */
    boolean checkDriverIsSelf() throws Exception;



    void dispatchTask(DriverEventDTO dto);

    void addWaitExecuteTask(JobTaskDTO taskDTO);

    void addIdleThread(ExecutorThreadDTO threadDTO);

    void dispatchJobEvent(DriverJobEventDTO dto);

    void dispatchExecutorEvent(DriverExecutorEventDTO dto);

    void dispatchEvent(DriverEventDTO dto);

}
