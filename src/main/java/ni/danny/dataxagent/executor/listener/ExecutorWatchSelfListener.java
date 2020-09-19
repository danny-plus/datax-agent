package ni.danny.dataxagent.executor.listener;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.executor.service.DataxExecutorService;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ExecutorWatchSelfListener implements CuratorCacheListener {

    @Autowired
    private DataxExecutorService dataxExecutorService;
    @Override
    public void event(Type type, ChildData oldData, ChildData data) {
        dataxExecutorService.process(type,oldData,data);
    }
}
