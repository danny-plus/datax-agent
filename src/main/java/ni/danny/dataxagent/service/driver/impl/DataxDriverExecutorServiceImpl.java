package ni.danny.dataxagent.service.driver.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.callback.DriverCallback;
import ni.danny.dataxagent.dto.event.DriverJobEventDTO;
import ni.danny.dataxagent.service.driver.DataxDriverExecutorService;
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
    public void executorCreatedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void executorRemovedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void threadCreatedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void threadUpdateWaitRecycleEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void threadUpdateReadyEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void threadRemovedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void threadTaskCreatedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void threadTaskUpdatedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void threadTaskRemovedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public boolean dispatchTask(String executor, String thread) {
        return false;
    }
}
