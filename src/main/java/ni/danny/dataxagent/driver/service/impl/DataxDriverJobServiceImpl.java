package ni.danny.dataxagent.driver.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.callback.DriverCallback;
import ni.danny.dataxagent.constant.DataxJobConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.driver.dto.event.DriverExecutorEventDTO;
import ni.danny.dataxagent.driver.dto.event.DriverJobEventDTO;
import ni.danny.dataxagent.driver.enums.DriverJobEventTypeEnum;
import ni.danny.dataxagent.driver.producer.DriverJobEventProducerWithTranslator;
import ni.danny.dataxagent.driver.service.DataxDriverJobService;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DataxDriverJobServiceImpl implements DataxDriverJobService {

    @Autowired
    private DriverJobEventProducerWithTranslator driverJobEventProducerWithTranslator;

    @Override
    public void scanJob(DriverCallback successCallback, DriverCallback failCallback) {

    }

    @Override
    public void dispatchJobEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        String[] pathInfo = data.getPath().split(ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG);
        switch (type.toString()){
            case "NODE_CREATED":
                if(pathInfo.length==3){
                    driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.JOB_CREATED
                            ,pathInfo[2],0,null));
                }else if(pathInfo.length==4){
                    driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_CREATED
                            ,pathInfo[2],Integer.parseInt(pathInfo[3]),null));
                }
                break;
            case "NODE_CHANGED":
                if(pathInfo.length==3){
                    if(DataxJobConstant.JOB_FINISH.equals(new String(data.getData()))){
                        driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.JOB_FINISHED
                                ,pathInfo[2],0,null));
                    }else if(DataxJobConstant.JOB_REJECT.equals(new String(data.getData()))){
                        driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.JOB_REJECTED
                                ,pathInfo[2],0,null));
                    }
                }else if(pathInfo.length==4){
                    if(DataxJobConstant.TASK_FINISH.equals(new String(data.getData()))){
                        driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_FINISHED
                                ,pathInfo[2],Integer.parseInt(pathInfo[3]),null));
                    }else if(DataxJobConstant.TASK_REJECT.equals(new String(data.getData()))){
                        driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_REJECTED
                                ,pathInfo[2],Integer.parseInt(pathInfo[3]),null));
                    }
                }else if(pathInfo.length==5){

                }
                break;
            case "NODE_DELETED":




                break;
        }
    }

    @Override
    public void jobCreatedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void jobRejectedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void jobFinishedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void taskCreatedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void taskRejectedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void taskFinishedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void taskThreadRejectedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public void taskThreadFinishedEvent(DriverExecutorEventDTO eventDTO) {

    }

    @Override
    public boolean dispatchTask(String jobId, String taskId) {
        return false;
    }
}
