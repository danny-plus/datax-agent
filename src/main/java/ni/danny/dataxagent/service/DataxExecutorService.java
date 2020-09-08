package ni.danny.dataxagent.service;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

public interface DataxExecutorService {
    /**
     * 执行器注册上线
     */
    void regist() throws Throwable;

    /**
     * 注册成功后，进行初始化
     */
    void init() throws Throwable;

    /**
     * 注册成功后，启动监听
     */
    void listen() throws Throwable;

    /**
     * 收到新任务
     */
    void receiveTask(ChildData data) throws Throwable;

    /**
     * 完成任务
     */
    void finishTask(ChildData data) throws Throwable;

    /**
     * 拒绝任务
     */
    void rejectTask(ChildData data) throws Throwable;

    /**
     *
     * 处理相关事件
     */
    void process(CuratorCacheListener.Type type, ChildData oldData, ChildData data);

    void createTask(ChildData data);
}
