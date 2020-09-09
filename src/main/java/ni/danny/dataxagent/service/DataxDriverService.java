package ni.danny.dataxagent.service;

import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.dto.DataxExecutorTaskDTO;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import java.util.List;

public interface DataxDriverService {
    /**
     * 注册成为Driver
     */
    void regist();

    /**
     * 启动事件监听
     */
    void listen();

    /**
     * 成为Driver后，进行初始化
     */
    void init();

    /**
     * 执行器巡检
     */
    void scanExecutor() throws Exception;

    /**
     * 任务巡检
     */
    void scanJob() throws Exception;

    /**
     * 检查下线执行器的任务执行情况
     */
    boolean checkOfflineExecutorThreadTask(DataxExecutorTaskDTO dto) throws Exception;

    /**
     * 新任务创建
     */
    DataxDTO createJob(DataxDTO dataxDTO);

    /**
     * 任务拆分
     */
    List<DataxDTO> splitJob(DataxDTO dataxDTO);

    /**
     * 任务分发 BY 执行器线程
     */
    boolean dispatchByExecutorThread(String executor,String thread);

    /**
     * 任务分发 BY 任务子模块
     * @return 已分配，未分配
     */
    boolean dispatchByJobTask(String jobId,String taskId);

    /**
     * 执行器事件分发器
     */
    void dispatchExecutorEvent(CuratorCacheListener.Type type,ChildData oldData,ChildData data);

    /**
     * 执行器上线
     */
    void executorUpEvent(ChildData data);

    /**
     * 执行器掉线
     */
    void executorDownEvent(ChildData oldData);


    /**
     * 任务执行器事件分发器
     */
    void dispatchJobExecutorEvent(CuratorCacheListener.Type type,ChildData oldData,ChildData data);

    /**
     * 执行器线程主动删除下挂任务
     */
    void executorThreadRemoveTask(DataxExecutorTaskDTO dto);


    /**
     * 停止监听 KAFKA中的日志
     * 递归方法，限制最大重试此时
     */
    void stopListenDataxLog();



}
