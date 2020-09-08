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

        checkJobExecutor();

        //每10分钟进行一次巡检
        driverEventList.add(new ZookeeperEventDTO("scanExecutor",null,null,null,10*60*1000));
    }

    /**
     * 检查所有的job/executor/executorIP:port/threadId
     */
    private void checkJobExecutor(){
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
        Set<String> tmpSet = new HashSet<>(jobList.size());
        if(jobList!=null && jobList.size()>0){
            for(String job:jobList){
                tmpSet.add(job);
            }
        }
        waitForExecutorJobTaskSet.clear();
        waitForExecutorJobTaskSet.addAll(tmpSet);
        //检查所有未完成的任务




        //10分钟进行一次巡检
        driverEventList.add(new ZookeeperEventDTO("scanJob",null,null,null,10*60*1000));
    }

    @Override
    public DataxDTO createJob(DataxDTO dataxDTO) {
        return null;
    }

    @Override
    public List<DataxDTO> splitJob(DataxDTO dataxDTO) {
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
