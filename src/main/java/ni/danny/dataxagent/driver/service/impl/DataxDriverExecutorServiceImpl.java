package ni.danny.dataxagent.driver.service.impl;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.driver.dto.ExecutorThreadDTO;
import ni.danny.dataxagent.driver.dto.event.DriverExecutorEventDTO;
import ni.danny.dataxagent.driver.enums.DriverExecutorEventTypeEnum;
import ni.danny.dataxagent.driver.enums.ExecutorThreadStatusEnums;
import ni.danny.dataxagent.driver.producer.DriverExecutorEventProducerWithTranslator;
import ni.danny.dataxagent.driver.service.DataxDriverExecutorService;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import ni.danny.dataxagent.enums.ExecutorTaskStatusEnum;
import ni.danny.dataxagent.service.DataxAgentService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Service
public class DataxDriverExecutorServiceImpl implements DataxDriverExecutorService {

    @Autowired
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    private DataxAgentService dataxAgentService;

    @Autowired
    private DataxDriverService dataxDriverService;

    @Autowired
    private DriverExecutorEventProducerWithTranslator driverExecutorEventProducerWithTranslator;

    @Value("${datax.executor.pool.maxPoolSize}")
    private int executorMaxPoolSize;

    @Autowired
    private Gson gson;

    @Override
    public void scanExecutor() {
        ZookeeperConstant.updateDriverExecutorEventHandlerStatus(ZookeeperConstant.STATUS_SLEEP);
        ZookeeperConstant.clearIdleThread();
        Set<ExecutorThreadDTO> tmpSet = new HashSet<>();
        try{
            List<String> list = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.EXECUTOR_ROOT_PATH);
            List<String> jobExecutorList = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH);
            for (String executor : jobExecutorList) {
                List<String> threads = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+executor);
                    if(threads!=null&&!threads.isEmpty()){
                        for(String thread:threads){
                            List<String> threadTasks = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                                    + ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG + executor + ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG + thread);
                            if (threadTasks == null || threadTasks.isEmpty()) {
                                if(list.contains(executor)){
                                    tmpSet.add(new ExecutorThreadDTO(executor, Integer.parseInt(thread)));
                                }else{
                                    zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                                            + ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG + executor + ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG + thread);
                                }
                            }else{
                                if(list.contains(executor)){
                                    zookeeperDriverClient.setData().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                                            + ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG + executor
                                            + ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG + thread,ExecutorThreadStatusEnums.READY.toString().getBytes());
                                    tmpSet.add(new ExecutorThreadDTO(executor, Integer.parseInt(thread)));
                                }else{
                                    zookeeperDriverClient.setData().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                                            + ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG + executor
                                            + ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG + thread,ExecutorThreadStatusEnums.WAITRECYCLE.toString().getBytes());
                                }
                            }
                        }
                    }

            }

            ZookeeperConstant.addIdleThread(tmpSet);
            dataxDriverService.dispatchTask();
        }catch (Exception ex){
            dispatchEvent(new DriverExecutorEventDTO(DriverExecutorEventTypeEnum.EXECUTOR_SCAN,null,0,null));
        }
        finally {
            ZookeeperConstant.updateDriverExecutorEventHandlerStatus(ZookeeperConstant.STATUS_RUNNING);
        }

    }

    @Override
    public void dispatchExecutorEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        String[] pathInfo = null;
        if(data!=null){
            pathInfo =data.getPath().split(ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG);
        }else if(oldData != null){
            pathInfo =oldData.getPath().split(ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG);
        }
        switch (type.toString()){
            case "NODE_CREATED":
                if(pathInfo.length==2){
                    dispatchEvent(new DriverExecutorEventDTO(DriverExecutorEventTypeEnum.EXECUTOR_UP,pathInfo[1],0,null));
                }else if(pathInfo.length==4){
                    //dispatchEvent(new DriverExecutorEventDTO(DriverExecutorEventTypeEnum.THREAD_CREATED,pathInfo[2],Integer.parseInt(pathInfo[3]),null) );
                }else if(pathInfo.length==5){
                    dispatchEvent(new DriverExecutorEventDTO(DriverExecutorEventTypeEnum.THREAD_TASK_CREATED,pathInfo[2],Integer.parseInt(pathInfo[3]),pathInfo[4]));
                }
                break;
            case "NODE_CHANGED":
                if(pathInfo.length==4){
                    if(ExecutorThreadStatusEnums.WAITRECYCLE.toString().equals(new String(data.getData()))){
                        dispatchEvent(new DriverExecutorEventDTO(DriverExecutorEventTypeEnum.THREAD_WAITRECYCLE
                                ,pathInfo[2],Integer.parseInt(pathInfo[3]),null) );
                    }else if(ExecutorThreadStatusEnums.READY.toString().equals(new String(data.getData()))){
                        dispatchEvent(new DriverExecutorEventDTO(DriverExecutorEventTypeEnum.THREAD_READY
                                ,pathInfo[2],Integer.parseInt(pathInfo[3]),null) );
                    }
                }else if(pathInfo.length==5){
                    dispatchEvent(new DriverExecutorEventDTO(DriverExecutorEventTypeEnum.THREAD_TASK_SET_TRACEID
                            ,pathInfo[2],Integer.parseInt(pathInfo[3]),pathInfo[4]) );
                }
                break;
            case "NODE_DELETED":
                if(pathInfo.length==2){
                    dispatchEvent(new DriverExecutorEventDTO(DriverExecutorEventTypeEnum.EXECUTOR_DOWN,pathInfo[1],0,null));
                }else if(pathInfo.length==4){
                    dispatchEvent(new DriverExecutorEventDTO(DriverExecutorEventTypeEnum.THREAD_REMOVED,pathInfo[2],Integer.parseInt(pathInfo[3]),null) );
                }else if(pathInfo.length==5){
                    dispatchEvent(new DriverExecutorEventDTO(DriverExecutorEventTypeEnum.THREAD_TASK_REMOVED,pathInfo[2],Integer.parseInt(pathInfo[3]),pathInfo[4]));
                }
                break;
            default:break;
        }
    }

    @Override
    public void executorCreatedEvent(DriverExecutorEventDTO eventDTO) {
        try{
            String jobExecutor = ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getExecutor();
            Stat jobExecutorStat = zookeeperDriverClient.checkExists().forPath(jobExecutor);
            if(jobExecutorStat == null){
                zookeeperDriverClient.create().withMode(CreateMode.PERSISTENT).forPath(jobExecutor);
            }

            List<ExecutorThreadDTO> list = new ArrayList<>();
                for(int i=0;i<executorMaxPoolSize;i++){
                    Stat threadStat = zookeeperDriverClient.checkExists().forPath(jobExecutor+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+i);
                    if(threadStat==null){
                        zookeeperDriverClient.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                                .forPath(jobExecutor+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+i,ExecutorThreadStatusEnums.READY.toString().getBytes());
                    }else{
                        zookeeperDriverClient.setData()
                                .forPath(jobExecutor+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+i,ExecutorThreadStatusEnums.READY.toString().getBytes());
                    }
                    list.add(new ExecutorThreadDTO(eventDTO.getExecutor(),i));
                }
                for(ExecutorThreadDTO dto:list){
                    dispatchEvent(new DriverExecutorEventDTO(DriverExecutorEventTypeEnum.THREAD_CREATED,dto.getExecutor(),dto.getThread(),null) );
                }
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dispatchEvent(eventDTO);
        }
    }

    @Override
    public void executorRemovedEvent(DriverExecutorEventDTO eventDTO) {
        try{
            String jobExecutor = ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getExecutor();
            Stat jobExecutorStat = zookeeperDriverClient.checkExists().forPath(jobExecutor);
            if(jobExecutorStat!=null){
                List<String> threads = zookeeperDriverClient.getChildren().forPath(jobExecutor);
                if(threads==null||threads.isEmpty()){
                    zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(jobExecutor);
                    return;
                }

                for (String thread:threads){
                    List<String> threadTasks = zookeeperDriverClient.getChildren().forPath(jobExecutor
                            +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+thread);
                    if(threadTasks==null||threadTasks.isEmpty()){
                        zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(jobExecutor
                                +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+thread);
                    }else{
                        zookeeperDriverClient.setData().forPath(jobExecutor
                                +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+thread
                                ,ExecutorThreadStatusEnums.WAITRECYCLE.toString().getBytes());
                    }
                }
            }
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dispatchEvent(eventDTO);
        }
    }

    @Override
    public void threadCreatedEvent(DriverExecutorEventDTO eventDTO) {
        dataxDriverService.addIdleThread(new ExecutorThreadDTO(eventDTO.getExecutor(),eventDTO.getThread()));
        dataxDriverService.dispatchTask();
    }

    @Override
    public void threadUpdateWaitRecycleEvent(DriverExecutorEventDTO eventDTO) {
        try{
            String threadPath = ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getExecutor()
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getThread();
            List<String> threadTasks = zookeeperDriverClient.getChildren().forPath(threadPath);
            if(threadTasks==null||threadTasks.isEmpty()){
                zookeeperDriverClient.delete().guaranteed().forPath(threadPath);
                return;
            }
            for(String task:threadTasks){
                String[] taskInfo = task.split(ZookeeperConstant.JOB_TASK_SPLIT_TAG);
                if(taskInfo.length==2){
                    String checkPath = ZookeeperConstant.JOB_LIST_ROOT_PATH+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG
                            +taskInfo[0]+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+taskInfo[1];
                            checkAndRemoveWaitRecycle(threadPath,checkPath);
                }else{
                    zookeeperDriverClient.delete().guaranteed().forPath(threadPath+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+task);
                }
            }
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dispatchEvent(eventDTO);
        }
    }

    private void checkAndRemoveWaitRecycle(String delPath,String path)throws Exception{
        Stat stat = zookeeperDriverClient.checkExists().forPath(path);
        if(stat == null){
            zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded()
                    .forPath(delPath);
            return ;
        }
        List<String> list = zookeeperDriverClient.getChildren().forPath(path);
        if(list==null||list.isEmpty()){
            zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded()
                    .forPath(delPath);
            return ;
        }

        String status = new String(zookeeperDriverClient.getData().forPath(path));
        if(ExecutorTaskStatusEnum.REJECT.getValue().equals(status)||ExecutorTaskStatusEnum.FINISH.getValue().equals(status)){
            zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded()
                    .forPath(delPath);
            return;
        }
        for(String tPath:list){
            checkAndRemoveWaitRecycle(delPath,path+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+tPath);
        }
    }

    @Override
    public void threadUpdateReadyEvent(DriverExecutorEventDTO eventDTO) {
        try{
            List<String> tasks = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getExecutor()
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getThread());
            if(tasks==null||tasks.isEmpty()){
                dataxDriverService.addIdleThread(new ExecutorThreadDTO(eventDTO.getExecutor(),eventDTO.getThread()));
                dataxDriverService.dispatchTask();
            }
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dispatchEvent(eventDTO);
        }
    }

    @Override
    public void threadRemovedEvent(DriverExecutorEventDTO eventDTO) {
        try{
            List<String> threads = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getExecutor());
            if(threads==null||threads.isEmpty()){
                zookeeperDriverClient.delete().guaranteed().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getExecutor());
            }
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dispatchEvent(eventDTO);
        }
    }

    @Override
    public void threadTaskCreatedEvent(DriverExecutorEventDTO eventDTO) {
       //doNothing
    }

    @Override
    public void threadTaskUpdatedEvent(DriverExecutorEventDTO eventDTO) {
        //doNothing
    }

    @Override
    public void threadTaskRemovedEvent(DriverExecutorEventDTO eventDTO) {
        try{
            String status = new String(zookeeperDriverClient.getData().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getExecutor()
                    +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getThread()));

            if(ExecutorThreadStatusEnums.WAITRECYCLE.toString().equals(status)){
                zookeeperDriverClient.delete().guaranteed().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getExecutor()
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getThread());
                return;
            }
            String[] taskInfo = eventDTO.getJobTask().split(ZookeeperConstant.JOB_TASK_SPLIT_TAG);
            if(taskInfo.length==2){
                Stat taskThreadStat = zookeeperDriverClient.checkExists().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+taskInfo[0]
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+taskInfo[1]
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getExecutor()
                        +ZookeeperConstant.JOB_TASK_SPLIT_TAG+eventDTO.getThread());
                if(taskThreadStat==null){
                    dataxDriverService.addIdleThread(new ExecutorThreadDTO(eventDTO.getExecutor(),eventDTO.getThread()));
                    dataxDriverService.dispatchTask();
                    return;
                }

                String taskThreadStatus = new String(zookeeperDriverClient.getData().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+taskInfo[0]
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+taskInfo[1]
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+eventDTO.getExecutor()
                        +ZookeeperConstant.JOB_TASK_SPLIT_TAG+eventDTO.getThread()));

                if(ExecutorTaskStatusEnum.REJECT.getValue().equals(taskThreadStatus)||ExecutorTaskStatusEnum.FINISH.getValue().equals(taskThreadStatus)){
                    dataxDriverService.addIdleThread(new ExecutorThreadDTO(eventDTO.getExecutor(),eventDTO.getThread()));
                    dataxDriverService.dispatchTask();
                }
            }
        }catch (Exception ex){
            eventDTO.setDelay(2*1000);
            dispatchEvent(eventDTO);
        }
    }

    @Override
    public void dispatchEvent(DriverExecutorEventDTO eventDTO) {
        if(eventDTO.getRetryNum()<10){
            eventDTO.updateRetry();
            driverExecutorEventProducerWithTranslator.onData(eventDTO);
        }
    }


}
