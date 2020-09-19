package ni.danny.dataxagent.executor.service;

import ni.danny.dataxagent.dto.DataxExecutorTaskDTO;
import ni.danny.dataxagent.executor.dto.event.ExecutorEventDTO;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

public interface DataxExecutorService {
    /**
     * 执行器注册上线
     */
    void regist() ;

    /**
     * 注册成功后，进行初始化
     */
    void init() throws Exception;

    /**
     * 注册成功后，启动监听
     */
    void listen() throws Exception;

    /**
     * 收到新任务
     */
    void receiveTask(DataxExecutorTaskDTO dataxExecutorTaskDTO);

    /**
     * 完成任务
     */
    void finishTask(DataxExecutorTaskDTO dataxExecutorTaskDTO) ;

    /**
     * 拒绝任务
     */
    void rejectTask(DataxExecutorTaskDTO dataxExecutorTaskDTO) ;

    /**
     *
     * 事件分发处理
     */
    void process(CuratorCacheListener.Type type, ChildData oldData, ChildData data);

    void dispatchEvent(ExecutorEventDTO dto);

}
