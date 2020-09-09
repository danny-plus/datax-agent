package ni.danny.dataxagent.service.impl;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.DataxJobConstant;
import ni.danny.dataxagent.dto.*;
import ni.danny.dataxagent.enums.ExecutorTaskStatusEnum;
import ni.danny.dataxagent.kafka.DataxLogConsumer;
import ni.danny.dataxagent.service.DataxDriverService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ni.danny.dataxagent.constant.ZookeeperConstant.*;
import ni.danny.dataxagent.constant.DataxJobConstant.*;
import org.springframework.web.client.RestTemplate;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static ni.danny.dataxagent.constant.DataxJobConstant.EXECUTOR_HEALTH_CHECK_URL;
import static ni.danny.dataxagent.constant.DataxJobConstant.HEALTH_UP;
import static ni.danny.dataxagent.constant.ZookeeperConstant.*;

@Slf4j
@Service
public class DataxDriverServiceImpl implements DataxDriverService {

    @Autowired
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    private ListenService listenService;

    @Autowired
    private AppInfoComp appInfoComp;

    @Autowired
    private DataxLogConsumer dataxLogConsumer;

    @Value("${datax.excutor.pool.maxPoolSize}")
    private int executorMaxPoolSize;

    @Value("${datax.maxCheckTimes}")
    private int maxCheckTimes;

    @Value("${datax.job.task.maxRejectTimes}")
    private int taskMaxRejectTimes;

    @Autowired
    private Gson gson;

    @Autowired
    private RestTemplate restTemplate;

    private String driverInfo(){
        return HTTP_PROTOCOL_TAG+appInfoComp.getIpAndPort();
    }

    @Override
    public void regist() {
        try{
            Stat driverStat = zookeeperDriverClient.checkExists().forPath(DRIVER_PATH);
            if(driverStat == null ){
                try{
                    zookeeperDriverClient.create().forPath(DRIVER_PATH,driverInfo().getBytes());
                    beDriver();
                }catch (Exception ex){
                    if(checkDriverIsSelf()){
                        beDriver();
                    }else{
                        notBeDriver();
                    }
                }
            }else{
                if(checkDriverIsSelf()){
                    beDriver();
                }else{
                    notBeDriver();
                }
            }
        }catch (Exception ignore){
            notBeDriver();
        }
    }

    private boolean checkDriverIsSelf() throws Exception{
        String driverData = new String(zookeeperDriverClient.getData().forPath(DRIVER_PATH));
        updateDriverName(null,driverData);
        if(driverInfo().equals(driverData)) {
            return true;
        }else{
            return false;
        }
    }

    private void beDriver(){
        updateDriverStatus(null,STATUS_INIT);
        updateDriverName(null,driverInfo());
        listen();
        init();
    }

    private void notBeDriver(){
        updateDriverStatus(null,STATUS_WATCH);
        listenService.watchDriver();
    }

    @Override
    public void listen() {
        listenService.driverWatchExecutor();
        listenService.driverWatchJobExecutor();
        listenService.driverWatchKafkaMsg();
    }

    @Override
    public void init() {
        try{
            //巡检执行器
            scanExecutor();
            //巡检任务
            scanJob();
            updateDriverStatus(STATUS_INIT,STATUS_RUNNING);
        }catch (Exception ex){
            regist();
        }
    }

    @Override
    public void scanExecutor() throws Exception {
        List<String> executors = zookeeperDriverClient.getChildren().forPath(EXECUTOR_ROOT_PATH);
        if(executors==null || executors.size()<=0){
            log.error("none executor online!");
            throw new Exception("none executor online!");
        }
        Set<String> tmpSet = new HashSet<>(executors.size());
        for(String executor:executors){
            tmpSet.add(executor);
        }

        onlineExecutorSet.clear();
        onlineExecutorSet.addAll(tmpSet);

        checkOfflineJobExecutor();

        checkOnlineExecutor();

        //每10分钟进行一次巡检
        driverEventList.add(new ZookeeperEventDTO("scanExecutor",null,null,null,10*60*1000));
    }

    /**
     * 检查所有的job/executor/executorIP:port/threadId
     */
    private void checkOfflineJobExecutor(){
        try{
            List<String> jobExecutors = zookeeperDriverClient.getChildren().forPath(JOB_EXECUTOR_ROOT_PATH);
            Set<String> tmpSet = new HashSet<>(onlineExecutorSet.size());
            tmpSet.addAll(onlineExecutorSet);
            if(jobExecutors!=null && jobExecutors.size()>0){
                for(String jobExecutor:jobExecutors){
                    if(!tmpSet.contains(jobExecutor)){
                        //不在在线清单中
                        if(checkExecutorHealth(jobExecutor)){

                            continue;
                        }
                        List<String> jobExecutorThreads = zookeeperDriverClient.getChildren().forPath(JOB_EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+jobExecutor);
                        int deletedThreadNum = 0;
                        if(jobExecutorThreads != null && jobExecutorThreads.size() > 0){
                            for(String thread:jobExecutorThreads){
                                List<String> threadTasks = zookeeperDriverClient.getChildren().forPath(JOB_EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+jobExecutor+ZOOKEEPER_PATH_SPLIT_TAG+thread);
                                if(threadTasks == null || threadTasks.size()<=0){
                                    deletedThreadNum ++;
                                }else{
                                    DataxExecutorTaskDTO dto = new DataxExecutorTaskDTO(jobExecutor,thread,threadTasks.get(0),0);
                                    if(checkOfflineExecutorThreadTask(dto)){
                                        deletedThreadNum ++;
                                    }
                                }
                            }
                        }
                        if(jobExecutorThreads.size() == deletedThreadNum){
                            //子线程节点已丢失，则删除此执行器
                            zookeeperDriverClient.delete().guaranteed().forPath(JOB_EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+jobExecutor);
                        }
                    }
                }
            }

                Iterator<String> iterator = tmpSet.iterator();
                while(iterator.hasNext()){
                    String executor = iterator.next();
                    Stat jobExecutorStat = zookeeperDriverClient.checkExists().forPath(JOB_EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+executor);
                    if(jobExecutorStat == null){
                        zookeeperDriverClient.create().forPath(JOB_EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+executor,(HTTP_PROTOCOL_TAG+executor).getBytes());
                    }
                    for(int i=0;i<executorMaxPoolSize;i++){
                        Stat jobExecutorThreadStat = zookeeperDriverClient.checkExists().forPath(JOB_EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+executor+ZOOKEEPER_PATH_SPLIT_TAG+i);
                        if(jobExecutorThreadStat == null){
                            zookeeperDriverClient.create().forPath(JOB_EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+executor+ZOOKEEPER_PATH_SPLIT_TAG+i,"".getBytes());
                        }
                    }
                }

        }catch (Exception ignore){}
    }


    @Override
    public boolean checkOfflineExecutorThreadTask(DataxExecutorTaskDTO dto){
        boolean result = false;

        try{
            if(!onlineExecutorSet.contains(dto.getExecutor())){
                List<String> tasks = zookeeperDriverClient.getChildren().forPath(JOB_EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG
                        +dto.getExecutor()+ZOOKEEPER_PATH_SPLIT_TAG+dto.getThread());
                 if(tasks==null||tasks.size()<=0){
                     zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(JOB_EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG
                             +dto.getExecutor()+ZOOKEEPER_PATH_SPLIT_TAG+dto.getThread());
                     result = true;
                 }else{
                     if(dto.getJobTask().equals(tasks.get(0))){
                         //检查KAFKA里是否有记录
                         if(DataxJobConstant.executorKafkaLogs.get(dto.getJobTask())!=null){
                             DataxLogDTO logDTO = DataxJobConstant.executorKafkaLogs.get(dto.getJobTask());
                             if(ExecutorTaskStatusEnum.REJECT.equals(logDTO.getStatus())
                             ||ExecutorTaskStatusEnum.FINISH.equals(logDTO.getStatus())){
                                 zookeeperDriverClient.setData().forPath(JOB_LIST_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG
                                         +dto.getTaskId()+ZOOKEEPER_PATH_SPLIT_TAG
                                         +dto.getTaskId()+ZOOKEEPER_PATH_SPLIT_TAG
                                         +dto.getExecutor()+JOB_TASK_SPLIT_TAG+dto.getThread(),logDTO.getStatus().getValue().getBytes());

                                 zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(JOB_EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG
                                         +dto.getExecutor()+ZOOKEEPER_PATH_SPLIT_TAG+dto.getThread());

                                 result = true;
                             }
                         }

                         if(!result){

                             int checkTimes = dto.getCheckTimes() +1;
                             if(checkTimes >= maxCheckTimes){
                                 zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(JOB_EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG
                                         +dto.getExecutor()+ZOOKEEPER_PATH_SPLIT_TAG+dto.getThread());
                                 result = true;
                             }

                             dto.setCheckTimes(checkTimes);
                             if(!result){
                              driverEventList.add(new ZookeeperEventDTO("checkOfflineExecutorThreadTask",null
                                      ,new ChildData(null,null,gson.toJson(dto).getBytes()),null,2*60*1000));
                             }
                         }
                     }
                 }
            }
        }catch (Exception ignore){}
        driverEventList.add(new ZookeeperEventDTO("checkOfflineExecutorThreadTask",null
                ,new ChildData(null,null,gson.toJson(dto).getBytes()),null,30*1000));
        return result;
    }

    /**
     * 检查所有的在线执行器，如果有空闲的，则进行任务分配
     */
    private void checkOnlineExecutor(){
        Set<String> tmpSet = new HashSet<>(onlineExecutorSet.size());
        tmpSet.addAll(onlineExecutorSet);
        Iterator<String> iterator = tmpSet.iterator();
        try{
            while(iterator.hasNext()){
                String executor = iterator.next();
                Stat executorStat = zookeeperDriverClient.checkExists().forPath(EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+executor);
                if(executorStat != null){
                    for(int i=0;i<executorMaxPoolSize;i++){
                        Stat threadStat = zookeeperDriverClient.checkExists().forPath(EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG
                                +executor+ZOOKEEPER_PATH_SPLIT_TAG+i);
                        if(threadStat != null){
                            List<String> threadTasks = zookeeperDriverClient.getChildren().forPath(EXECUTOR_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG
                                    +executor+ZOOKEEPER_PATH_SPLIT_TAG+i);
                            if(threadTasks == null ||threadTasks.size()<=0){
                                dispatchByExecutorThread(executor,i+"");
                            }
                        }
                    }
                }else{
                    if(checkExecutorHealth(executor)){
                        onlineExecutorSet.remove(executor);
                    }
                }
            }
        }catch (Exception ignore){}
    }

    private boolean checkExecutorHealth(String executor){
        ActuatorHealthDTO healthDTO = restTemplate.getForObject(HTTP_PROTOCOL_TAG+executor+ZOOKEEPER_PATH_SPLIT_TAG+EXECUTOR_HEALTH_CHECK_URL,ActuatorHealthDTO.class);
        if(!HEALTH_UP.equals(healthDTO.getStatus().toUpperCase())){
            return false;
        }
        return true;

    }

    @Override
    public void scanJob() throws Exception  {
        //扫描全部任务
        List<String> jobList = zookeeperDriverClient.getChildren().forPath(JOB_LIST_ROOT_PATH);


        waitForExecutorJobTaskSet.clear();
        waitForExecutorJobTaskSet.addAll(jobList);
        //检查所有未完成的任务
        try{
            if(jobList!=null && jobList.size()>0){
                for(String job:jobList){
                    checkJob(job);
                }
            }
        }catch (Exception ignore){}

        //10分钟进行一次巡检
        driverEventList.add(new ZookeeperEventDTO("scanJob",null,null,null,10*60*1000));
    }

    /**
     * 检查单个JOB下的所有TASK
     * @param jobId
     */
    private void checkJob(String jobId) throws Exception{
        Stat jobStat = zookeeperDriverClient.checkExists().forPath(JOB_LIST_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+jobId);
        if(jobStat!=null){
            List<String> taskList = zookeeperDriverClient.getChildren().forPath(JOB_LIST_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+jobId);
            boolean jobDel = false;
            int taskDelNum = 0;
            if(taskList!=null&&taskList.size()>0){
                for(String task:taskList){
                    if(checkJobTask(jobId,task)){
                        taskDelNum = taskDelNum+1;
                    }
                }
            }

            if(taskDelNum == taskList.size()){
                String jobData = new String(zookeeperDriverClient.getData().forPath(JOB_LIST_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+jobId));
                if(DataxJobConstant.JOB_FINISH.toString().equals(jobData)){
                    zookeeperDriverClient.delete().guaranteed().forPath(JOB_LIST_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+jobId);
                }else{
                    DataxDTO jobDto = gson.fromJson(jobData,DataxDTO.class);
                    List<DataxDTO> dataxList = splitJob(jobDto);
                    for(DataxDTO taskDto : dataxList){
                        dispatchByJobTask(taskDto.getJobId(),taskDto.getTaskId());
                    }
                }
            }
        }
    }

    private boolean checkJobTask(String jobId,String taskId)throws Exception{
        boolean removeTag = true;
        Stat taskStat = zookeeperDriverClient.checkExists().forPath(JOB_LIST_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+jobId
                +ZOOKEEPER_PATH_SPLIT_TAG+taskId);
        if(taskStat!=null){
            List<String> taskThreads = zookeeperDriverClient.getChildren().forPath(JOB_LIST_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+jobId
                    +ZOOKEEPER_PATH_SPLIT_TAG+taskId);
            int rejectNum = 0;
            int finishNum = 0;
            if(taskThreads!=null&& taskThreads.size()>0){
                for(String taskThread:taskThreads){
                    switch (checkJobTaskThread(jobId,taskId,taskThread).getValue()){
                       case "FINISH": finishNum = finishNum+1; break;
                       case "REJECT": rejectNum = rejectNum+1;break;
                        default: break;
                    }
                }
            }

            if(finishNum>0||rejectNum >= taskMaxRejectTimes){
                removeTag = true;
            }else{
                dispatchByJobTask(jobId,taskId);
                removeTag = false;
            }
        }
        return removeTag;
    }

    private ExecutorTaskStatusEnum checkJobTaskThread(String jobId,String taskId,String executorThread) throws Exception{
        Stat taskThreadStat = zookeeperDriverClient.checkExists().forPath(JOB_LIST_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+jobId
                +ZOOKEEPER_PATH_SPLIT_TAG+taskId+ZOOKEEPER_PATH_SPLIT_TAG+executorThread);
        if(taskThreadStat != null){
            String taskThreadStatus = new String(zookeeperDriverClient.getData().forPath(JOB_LIST_ROOT_PATH+ZOOKEEPER_PATH_SPLIT_TAG+jobId
                    +ZOOKEEPER_PATH_SPLIT_TAG+taskId+ZOOKEEPER_PATH_SPLIT_TAG+executorThread));

            ExecutorTaskStatusEnum.getTaskStatusByValue(taskThreadStatus);
        }

        return  ExecutorTaskStatusEnum.UNKOWN;
    }

    @Override
    public DataxDTO createJob(DataxDTO dataxDTO) {

        return null;
    }

    @Override
    public List<DataxDTO> splitJob(DataxDTO dataxDTO) {
        //拆分并创建节点

        return null;
    }

    @Override
    public void dispatchByExecutorThread(String executor, String thread) {

    }

    @Override
    public void dispatchByJobTask(String jobId, String taskId) {

    }

    @Override
    public void dispatchExecutorEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {

    }

    @Override
    public void executorUpEvent(ChildData data) {

    }

    @Override
    public void executorDownEvent(ChildData oldData) {

    }

    @Override
    public void dispatchJobExecutorEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {

    }

    @Override
    public void executorThreadRemoveTask(DataxExecutorTaskDTO dto) {

    }

    @Override
    public void stopListenDataxLog() {
        try{
            dataxLogConsumer.stopListen();
        }catch (Exception ignore){}
    }
}
