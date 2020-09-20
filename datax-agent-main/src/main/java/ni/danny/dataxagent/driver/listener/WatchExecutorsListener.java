package ni.danny.dataxagent.driver.listener;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.driver.service.DataxDriverExecutorService;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
@Slf4j
@Component
public class WatchExecutorsListener implements CuratorCacheListener {

    @Autowired
    private DataxDriverExecutorService dataxDriverExecutorService;

    @Override
    public void event(Type type, ChildData oldData, ChildData data) {
        dataxDriverExecutorService.dispatchExecutorEvent(type,oldData,data);
    }
}
