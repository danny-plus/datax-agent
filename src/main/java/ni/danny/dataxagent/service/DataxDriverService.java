package ni.danny.dataxagent.service;

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



}
