package ni.danny.dataxagent.service;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

public interface DataxExecutorService {
    void init();

    /**
     * 注册
     */
    void regist();

    /**
     * 处理相关事件
     */
    void process(CuratorCacheListener.Type type, ChildData oldData, ChildData data);

    void createTask(ChildData data);
}
