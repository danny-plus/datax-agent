package ni.danny.dataxagent.driver.service.impl;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.DataxJobConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.driver.dto.JobTaskDTO;
import ni.danny.dataxagent.driver.dto.event.DriverEventDTO;
import ni.danny.dataxagent.driver.dto.event.DriverJobEventDTO;
import ni.danny.dataxagent.driver.enums.DriverJobEventTypeEnum;
import ni.danny.dataxagent.driver.service.DataxDriverJobService;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.enums.ExecutorTaskStatusEnum;
import ni.danny.dataxagent.service.DataxAgentService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Service
public class DataxDriverJobServiceImpl implements DataxDriverJobService {

    @Autowired
    @Lazy
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    @Lazy
    private DataxAgentService dataxAgentService;

    @Autowired
    @Lazy
    private DataxDriverService dataxDriverService;

    @Autowired
    private Gson gson;

    @Override
    public void scanJob() {
        ZookeeperConstant.updateDriverJobEventHandlerStatus(ZookeeperConstant.STATUS_SLEEP);
        ZookeeperConstant.waitForExecuteTaskSet.clear();
        Set<JobTaskDTO> tmpSet = new HashSet<>();
        try{
            List<String> jobList = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH);
            for(String jobId:jobList){
                List<String> taskList = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+jobId);
                for (String task:taskList){
                    List<String> exeThreads = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                            +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+jobId+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+task);
                    if(exeThreads.isEmpty()){
                        tmpSet.add(new JobTaskDTO(jobId,Integer.parseInt(task)));
                    }
                }
            }
            ZookeeperConstant.waitForExecuteTaskSet.addAll(tmpSet);
            dataxDriverService.dispatchEvent(new DriverEventDTO(DriverJobEventTypeEnum.TASK_DISPATCH).delay(2000));
        }catch (Exception ex){
            dataxDriverService.dispatchJobEvent(new DriverJobEventDTO(DriverJobEventTypeEnum.JOB_SCAN
                    ,null,0,null,null,2*1000));

        }finally {
            ZookeeperConstant.updateDriverJobEventHandlerStatus(ZookeeperConstant.STATUS_RUNNING);
        }
    }

    @Override
    public void dispatchJobEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        String pathStr = "";
        String[] pathInfo = null;
        if(data!=null){
            pathStr =data.getPath();
        }else if(oldData != null){
            pathStr =oldData.getPath();
        }
        pathInfo = pathStr.split(ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG);
        log.debug("type=[{}],data=[{}]",type,pathStr);
        switch (type.toString()){
            case "NODE_CREATED":
                if(pathInfo.length==4){
                    dataxDriverService.dispatchJobEvent(new DriverJobEventDTO(DriverJobEventTypeEnum.JOB_CREATED
                            ,pathInfo[3],0,null,new String(data.getData())));
                }else if(pathInfo.length==5){
                    //不由ZOOKEEPER触发事件
//                    driverJobEventProducerWithTranslator.onData(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_CREATED
//                            ,pathInfo[3],Integer.parseInt(pathInfo[4]),null,new String(data.getData())));
                }
                break;
            case "NODE_CHANGED":
                if(pathInfo.length==4){
                    if(DataxJobConstant.JOB_FINISH.equals(new String(data.getData()))){
                        dataxDriverService.dispatchJobEvent(new DriverJobEventDTO(DriverJobEventTypeEnum.JOB_FINISHED
                                ,pathInfo[3],0,null,null));
                    }else if(DataxJobConstant.JOB_REJECT.equals(new String(data.getData()))){
                        dataxDriverService.dispatchJobEvent(new DriverJobEventDTO(DriverJobEventTypeEnum.JOB_REJECTED
                                ,pathInfo[3],0,null,null));
                    }
                }else if(pathInfo.length==5){
                    if(DataxJobConstant.TASK_FINISH.equals(new String(data.getData()))){
                        dataxDriverService.dispatchJobEvent(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_FINISHED
                                ,pathInfo[3],Integer.parseInt(pathInfo[4]),null,null));
                    }else if(DataxJobConstant.TASK_REJECT.equals(new String(data.getData()))){
                        dataxDriverService.dispatchJobEvent(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_REJECTED
                                ,pathInfo[3],Integer.parseInt(pathInfo[4]),null,null));
                    }
                }else if(pathInfo.length==6){
                    if(DataxJobConstant.TASK_FINISH.equals(new String(data.getData()))){
                        dataxDriverService.dispatchJobEvent(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_THREAD_FINISHED
                                ,pathInfo[3],Integer.parseInt(pathInfo[4]),pathInfo[5],null));
                    }else if(DataxJobConstant.TASK_REJECT.equals(new String(data.getData()))){
                        dataxDriverService.dispatchJobEvent(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_THREAD_REJECTED
                                ,pathInfo[3],Integer.parseInt(pathInfo[4]),pathInfo[5],null));
                    }
                }
                break;
            case "NODE_DELETED":
//                log.info("");

                break;
            default:break;
        }
    }

    @Override
    public void jobCreatedEvent(DriverJobEventDTO eventDTO) {
        DataxDTO jobDatax = gson.fromJson(eventDTO.getDataxJson(),DataxDTO.class);

        List<DataxDTO> taskList = dataxAgentService.splitDataxJob(jobDatax);

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
                    }else{
                        zookeeperDriverClient.delete().guaranteed().forPath(taskPath);
                    }
                }
                if(createTaskFlag){
                    zookeeperDriverClient.create().withMode(CreateMode.PERSISTENT).forPath(taskPath,gson.toJson(taskDto).getBytes());
                }
            }
            for(DataxDTO taskDto:taskList){
                dataxDriverService.dispatchJobEvent(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_CREATED
                        ,taskDto.getJobId(),taskDto.getTaskId(),null,gson.toJson(taskDto)));
            }
        }catch (Exception exception){
            dataxDriverService.dispatchJobEvent(eventDTO);
        }
    }


    @Override
    public void jobRejectedEvent(DriverJobEventDTO eventDTO) {
        dataxAgentService.rejectJob(eventDTO.getJobId());
        try{
            zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+
                    ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId());
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dataxDriverService.dispatchJobEvent(eventDTO);
        }
    }

    @Override
    public void jobFinishedEvent(DriverJobEventDTO eventDTO) {
        dataxAgentService.finishJob(eventDTO.getJobId());
        try{
            zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+
                    ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId());
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dataxDriverService.dispatchJobEvent(eventDTO);
        }
    }

    @Override
    public void taskCreatedEvent(DriverJobEventDTO eventDTO) {
        dataxDriverService.addHandlerResource(null,new JobTaskDTO(eventDTO.getJobId(),eventDTO.getTaskId()),null);
    }

    @Override
    public void taskRejectedEvent(DriverJobEventDTO eventDTO) {
        dataxAgentService.rejectTask(eventDTO.getJobId(),eventDTO.getTaskId());
        try{
            List<String> tasks = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId());
            int rejectTaskNum = 0;
            for(String task:tasks){
                String data = new String(zookeeperDriverClient.getData().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId()
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+task));
                if(ExecutorTaskStatusEnum.REJECT.getValue().equals(data)){
                    rejectTaskNum++;
                }
            }
            if(rejectTaskNum>tasks.size()/2){
                zookeeperDriverClient.setData().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId()
                        ,ExecutorTaskStatusEnum.REJECT.getValue().getBytes());
            }
            dataxDriverService.addHandlerResource(null,new JobTaskDTO(eventDTO.getJobId(),eventDTO.getTaskId()),null);
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dataxDriverService.dispatchJobEvent(eventDTO);
        }
    }

    @Override
    public void taskFinishedEvent(DriverJobEventDTO eventDTO) {
        dataxAgentService.finishTask(eventDTO.getJobId(),eventDTO.getTaskId());
        try{
            List<String> tasks = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId());
            int finishTaskNum = 0;
            for(String task:tasks){
                String data = new String(zookeeperDriverClient.getData().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId()
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+task));
                if(ExecutorTaskStatusEnum.FINISH.getValue().equals(data)){
                    finishTaskNum++;
                }
            }

            if(finishTaskNum==tasks.size()){
                zookeeperDriverClient.setData().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                                +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId()
                        ,ExecutorTaskStatusEnum.FINISH.getValue().getBytes());
            }
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dataxDriverService.dispatchJobEvent(eventDTO);
        }
    }

    @Override
    public void taskThreadRejectedEvent(DriverJobEventDTO eventDTO) {
        try{
            List<String> taskThreads = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId()
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getTaskId());
            int rejectNum = 0;
            for(String taskThread:taskThreads){
                String data =new String( zookeeperDriverClient.getData().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId()
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getTaskId()
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+taskThread));
                if(ExecutorTaskStatusEnum.REJECT.getValue().equals(data)){
                    rejectNum++;
                }
            }
            if(rejectNum>4){
                zookeeperDriverClient.setData().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                                +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId()
                                +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getTaskId()
                        ,ExecutorTaskStatusEnum.REJECT.getValue().getBytes());
            }else{
                dataxDriverService.addHandlerResource(null,new JobTaskDTO(eventDTO.getJobId(),eventDTO.getTaskId()),null);
            }
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dataxDriverService.dispatchJobEvent(eventDTO);
        }

    }

    @Override
    public void taskThreadFinishedEvent(DriverJobEventDTO eventDTO) {
        try{
            zookeeperDriverClient.setData().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getJobId()
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getTaskId()
                    ,ExecutorTaskStatusEnum.FINISH.getValue().getBytes());
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dataxDriverService.dispatchJobEvent(eventDTO);
        }
    }


}
