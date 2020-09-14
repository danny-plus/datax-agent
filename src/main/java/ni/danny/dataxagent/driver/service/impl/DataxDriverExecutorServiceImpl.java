package ni.danny.dataxagent.driver.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.callback.DriverCallback;
import ni.danny.dataxagent.driver.dto.event.DriverExecutorEventDTO;
import ni.danny.dataxagent.driver.service.DataxDriverExecutorService;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DataxDriverExecutorServiceImpl implements DataxDriverExecutorService {
    @Override
    public void scanExecutor(DriverCallback successCallback, DriverCallback failCallback) {

    }

    @Override
    public void dispatchExecutorEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {

    }

    @Override
    public void executorCreatedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void executorRemovedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void threadCreatedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void threadUpdateWaitRecycleEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void threadUpdateReadyEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void threadRemovedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void threadTaskCreatedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void threadTaskUpdatedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void threadTaskRemovedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public boolean dispatchTask(String executor, String thread) {
        return false;
    }
}
