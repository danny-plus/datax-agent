package ni.danny.dataxagent.service;

import ni.danny.dataxagent.dto.DataxDTO;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

public interface DataxDriverService {
    /**
     *
     */
    void init();

    /**
     * 注册成为Driver
     */
    void regist();

    void managerExecutor(CuratorCacheListener.Type type, ChildData oldData, ChildData data);

    void executorUp(ChildData data);

    void executorDown(ChildData oldData);

    /**
     * 检查任务分配情况
     * @param jobId
     */
    DataxDTO checkJob(String jobId) throws Exception;

    void splitJob(String jobId,DataxDTO jobDto) throws Exception;

    /**
     * 将任务随机分配至执行器
     * @param jobId
     * @param taskId
     */
    void distributeTask(String jobId,String taskId) throws Exception;

    /**
     * 给执行器分配具体任务
     * @param executorPath
     */
    void distributeTask(String executorPath) throws Exception;

    /**
     * 调度器管理，任务执行器节点的变化
     * @param type
     * @param oldData
     * @param data
     */
    void manageJobExecutorChange(CuratorCacheListener.Type type, ChildData oldData, ChildData data);

    /**
     * 调度器发现任务执行器移除了具体任务-表示任务已完成，执行器进行任务删除
     * @param oldData
     */
    void jobExecutorRemoveTask(ChildData oldData);

    /**
     * 如果是当前JOB的最后一个任务，则删除JOB，否则什么都不做
     * @param jobId
     * @param taskId
     */
    void removeJobWhenLastTask(String jobId,String taskId) throws Exception;

    /**
     * 未成功注册则暂停监听KAFKA
     */
    void stopListenKafka(int num);



}
