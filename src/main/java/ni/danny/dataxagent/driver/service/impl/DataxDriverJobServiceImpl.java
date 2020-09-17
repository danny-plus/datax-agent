package ni.danny.dataxagent.driver.service.impl;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.callback.DriverCallback;
import ni.danny.dataxagent.constant.DataxJobConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.driver.dto.event.DriverJobEventDTO;
import ni.danny.dataxagent.driver.enums.DriverJobEventTypeEnum;
import ni.danny.dataxagent.driver.producer.DriverJobEventProducerWithTranslator;
import ni.danny.dataxagent.driver.service.DataxDriverJobService;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.service.DataxJobSpiltContextService;
import ni.danny.dataxagent.service.DataxJobSpiltStrategy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Service
public class DataxDriverJobServiceImpl implements DataxDriverJobService {

    @Autowired
    private DriverJobEventProducerWithTranslator driverJobEventProducerWithTranslator;

    @Autowired
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    private DataxJobSpiltContextService dataxJobSpiltContextService;

    @Autowired
    private Gson gson;

    @Override
    public void scanJob(DriverCallback successCallback, DriverCallback failCallback) {
        ZookeeperConstant.updateDriverJobEventHandlerStatus(ZookeeperConstant.STATUS_SLEEP);
        ZookeeperConstant.waitForExecutorJobTaskSet.clear();
        Set<String> tmpSet = new HashSet<>();
        try{
            List<String> jobList = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH);
            for(String jobId:jobList){
                List<String> taskList = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+jobId);
                for (String task:taskList){
                    List<String> exeThreads = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                            +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+jobId+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+task);
                    if(exeThreads.isEmpty()){
                        tmpSet.add(jobId+ZookeeperConstant.JOB_TASK_SPLIT_TAG+task);
                    }
                }
            }
            ZookeeperConstant.waitForExecutorJobTaskSet.addAll(tmpSet);
        }catch (Exception ex){
             driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.JOB_SCAN
                     ,null,0,null,null,2*1000) );

        }finally {
            ZookeeperConstant.updateDriverJobEventHandlerStatus(ZookeeperConstant.STATUS_RUNNING);
        }
    }

    @Override
    public void dispatchJobEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        String[] pathInfo = null;
        if(data!=null){
            pathInfo =data.getPath().split(ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG);
        }else if(oldData != null){
            pathInfo =oldData.getPath().split(ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG);
        }
     //   log.info("type=[{}],data=[{}]",type,pathInfo);
        switch (type.toString()){
            case "NODE_CREATED":
                if(pathInfo.length==4){
                    driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.JOB_CREATED
                            ,pathInfo[3],0,null,new String(data.getData())));
                }else if(pathInfo.length==5){
                    driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_CREATED
                            ,pathInfo[3],Integer.parseInt(pathInfo[4]),null,new String(data.getData())));
                }
                break;
            case "NODE_CHANGED":
                if(pathInfo.length==4){
                    if(DataxJobConstant.JOB_FINISH.equals(new String(data.getData()))){
                        driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.JOB_FINISHED
                                ,pathInfo[3],0,null,null));
                    }else if(DataxJobConstant.JOB_REJECT.equals(new String(data.getData()))){
                        driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.JOB_REJECTED
                                ,pathInfo[3],0,null,null));
                    }
                }else if(pathInfo.length==5){
                    if(DataxJobConstant.TASK_FINISH.equals(new String(data.getData()))){
                        driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_FINISHED
                                ,pathInfo[3],Integer.parseInt(pathInfo[4]),null,null));
                    }else if(DataxJobConstant.TASK_REJECT.equals(new String(data.getData()))){
                        driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_REJECTED
                                ,pathInfo[3],Integer.parseInt(pathInfo[4]),null,null));
                    }
                }else if(pathInfo.length==6){
                    if(DataxJobConstant.TASK_FINISH.equals(new String(data.getData()))){
                        driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_THREAD_FINISHED
                                ,pathInfo[3],Integer.parseInt(pathInfo[4]),pathInfo[5],null));
                    }else if(DataxJobConstant.TASK_REJECT.equals(new String(data.getData()))){
                        driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_THREAD_REJECTED
                                ,pathInfo[3],Integer.parseInt(pathInfo[4]),pathInfo[5],null));
                    }
                }
                break;
            case "NODE_DELETED":
//                log.info("");

                break;
        }
    }

    @Override
    public void jobCreatedEvent(DriverJobEventDTO eventDTO) {
        DataxDTO jobDatax = gson.fromJson(eventDTO.getDataxJson(),DataxDTO.class);

        List<DataxDTO> taskList = spiltJob(jobDatax);

        try{
            for(DataxDTO taskDto:taskList){
                String taskPath = ZookeeperConstant.JOB_LIST_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+jobDatax.getJobId()+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG
                        +taskDto.getTaskId();
                Stat taskStat = zookeeperDriverClient.checkExists().forPath(taskPath);

                boolean createTaskFlag = true;
                if(taskStat!=null){
                    List<String> taskChildren = zookeeperDriverClient.getChildren().forPath(taskPath);
                    if(taskChildren!=null&&!taskChildren.isEmpty()){
                        createTaskFlag = false;
                    }
                }
                if(createTaskFlag){
                    zookeeperDriverClient.create().withMode(CreateMode.PERSISTENT).forPath(taskPath,gson.toJson(taskDto).getBytes());
                }
            }
        }catch (Exception exception){

            dispatchEvent(eventDTO);
        }
    }

    private List<DataxDTO> spiltJob(DataxDTO dataxDTO){
        return dataxJobSpiltContextService.splitDataxJob(dataxDTO.getSplitStrategy().getType(),dataxDTO.getJobId(),dataxDTO);
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

    @Override
    public void dispatchEvent(DriverJobEventDTO dto) {
        if(dto.getRetryNum()<10){
            dto.updateRetry();
            driverJobEventProducerWithTranslator.onData(dto);
        }
    }
}
