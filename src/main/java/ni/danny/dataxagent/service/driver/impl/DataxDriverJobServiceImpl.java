package ni.danny.dataxagent.service.driver.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.callback.DriverCallback;
import ni.danny.dataxagent.dto.event.DriverJobEventDTO;
import ni.danny.dataxagent.service.driver.DataxDriverJobService;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DataxDriverJobServiceImpl implements DataxDriverJobService {
    @Override
    public void scanJob(DriverCallback successCallback, DriverCallback failCallback) {

    }

    @Override
    public void dispatchJobExecutorEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {

    }

    @Override
    public void jobCreatedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void jobRejectedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void jobFinishedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void taskCreatedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void taskRejectedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void taskFinishedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void taskThreadRejectedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public void taskThreadFinishedEvent(DriverJobEventDTO eventDTO) {

    }

    @Override
    public boolean dispatchTask(String jobId, String taskId) {
        return false;
    }
}
