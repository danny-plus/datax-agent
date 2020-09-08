package ni.danny.dataxagent.service.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.DataxJobConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.dto.ActuatorHealthDTO;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.dto.ZookeeperEventDTO;
import ni.danny.dataxagent.kafka.DataxLogConsumer;
import ni.danny.dataxagent.service.DataxDriverService;
import ni.danny.dataxagent.service.DataxJobSpiltContextService;
import ni.danny.dataxagent.service.DriverEventReplayService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static ni.danny.dataxagent.constant.ZookeeperConstant.*;
import static ni.danny.dataxagent.constant.ZookeeperConstant.updateDriverName;

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
    private Gson gson;

    @Autowired
    private DataxLogConsumer dataxLogConsumer;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private DataxJobSpiltContextService dataxJobSpiltContextService;


    @Value("${datax.excutor.pool.maxPoolSize}")
    private int maxPoolSize;

    @Override
    public void init() {
        if(!STATUS_INIT.equals(ZookeeperConstant.updateDriverStatus(null,STATUS_INIT))){
            log.error("driver init failed, the DRIVER STATUS is wrong");
            return ;
        }
        driverEventList.clear();
        idleExecutorThreadSet.clear();

        updateDriverName(driverName,appInfoComp.getIpAndPort());

        DataxJobConstant.dataxDTOS.clear();
        ZookeeperConstant.onlineExecutorSet.clear();
        try{
            //扫描所有的执行器，在本地维护一个完整的执行器在线表，如果在线，则放入一个上线事件
            List<String> executorPaths = zookeeperDriverClient.getChildren().forPath(EXECUTOR_ROOT_PATH);
            if(executorPaths!=null&&executorPaths.size()>0){
                for(String path:executorPaths){
                    String onlineExecutorPath = path.replace(EXECUTOR_ROOT_PATH,"");
                    onlineExecutorSet.add(onlineExecutorPath);
                    driverEventList.add(new ZookeeperEventDTO("managerExecutor",CuratorCacheListener.Type.NODE_CREATED,null,new ChildData(path,new Stat(),"".getBytes()),2*1000));
                }
            }
            //扫描所有的任务执行器，如果在线执行器SET中没有，则放入一个下线事件
            List<String> jobExecutorPaths = zookeeperDriverClient.getChildren().forPath(JOB_EXECUTOR_ROOT_PATH);
            if(jobExecutorPaths!=null&&jobExecutorPaths.size()>0){
                for(String path:jobExecutorPaths){
                    String jobExecutorPath = path.replace(JOB_EXECUTOR_ROOT_PATH,"");
                    log.info("jobExecutorPath==[{}]",jobExecutorPath);
                    if(!onlineExecutorSet.contains(jobExecutorPath)){
                        driverEventList.add(new ZookeeperEventDTO("managerExecutor",CuratorCacheListener.Type.NODE_DELETED,null,new ChildData(path,new Stat(),"".getBytes()),2*1000));
                    }
                }
            }

        }catch (Exception ex){
            //TODO:扫描失败

        }
        //扫描获取所有JOB及其信息，在本地维护一个HASHMAP
        try{
            List<String> jobPaths = zookeeperDriverClient.getChildren().forPath(JOB_LIST_ROOT_PATH);
            log.info("now jobs is ==>[{}]",jobPaths);
            if(jobPaths!=null&&jobPaths.size()>0){
                Set<DataxDTO> dataxJobSets = new HashSet<>(jobPaths.size());
                for(String jobId:jobPaths){
                    DataxDTO tmpDatax = checkJob(jobId);
                    if(!dataxJobSets.add(tmpDatax)){
                        log.info("there have the same dataxJob info ,then job info is =[{}]",tmpDatax);
                    }
                }
                DataxJobConstant.dataxDTOS.addAll(dataxJobSets);
            }
        }catch (Exception e){
            //TODO: 扫描失败
        }

        if(STATUS_RUNNING.equals(ZookeeperConstant.updateDriverStatus(STATUS_INIT,STATUS_RUNNING))){
            listenService.driverWatchKafkaMsg();
        }
    }


    @Override
    public void regist() {
        try{
            DataxJobConstant.executorKafkaLogs.clear();
            zookeeperDriverClient.create().withMode(CreateMode.EPHEMERAL).forPath(ZookeeperConstant.DRIVER_PATH, ("http://"+appInfoComp.getIpAndPort()).getBytes());
            listenService.driverWatchExecutor();
            listenService.driverWatchJobExecutor();
            init();
        }catch (Exception ex){
            try{
                Stat stat =  zookeeperDriverClient.checkExists().forPath(ZookeeperConstant.DRIVER_PATH);
                if(stat == null){
                    regist();
                }else{
                    String info =  new String(zookeeperDriverClient.getData().forPath(ZookeeperConstant.DRIVER_PATH));
                    if(("http://"+appInfoComp.getIpAndPort()).equals(info)){
                        listenService.driverWatchExecutor();
                        listenService.driverWatchJobExecutor();
                        init();
                    }else{
                        updateDriverName(driverName,null);
                        stopListenKafka(0);
                        listenService.watchDriver();
                    }
                }

            }catch (Exception ignore){}
            updateDriverName(driverName,null);
            stopListenKafka(0);
            listenService.watchDriver();
        }
    }

    @Override
    public void managerExecutor(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        //调度器初始化未完成，则不进行相关工作调度
        if(!STATUS_RUNNING.equals(ZookeeperConstant.driverStatus)){
            //调度器初始化未完成，则不进行相关工作调度
            //将初始化期间的事件信息塞入队列中
            log.info("driver status is not running ==>");
            driverEventList.add(new ZookeeperEventDTO("managerExecutor",type,oldData,data,2*1000));
            return ;
        }
        switch (type.toString()){
            case "NODE_CREATED":executorUp(data); break;
            case "NODE_DELETED":executorDown(oldData);break;
            default: log.info("other child event "+type);break;
        }
    }

    @Override
    public void executorUp(ChildData data) {

        //检查节点当前是否存在
        try{
            Stat stat = zookeeperDriverClient.checkExists().forPath(data.getPath());
            if(stat == null){
                //重放时，将上线-下线的记录，转换成了，下线->上线，其实执行器已经离线
                return ;
            }
        }catch (Exception ex){
            log.error("check executor is up failed, because of the error =>",ex);
            return;
        }
        String executorInfo = data.getPath().replace(ZookeeperConstant.EXECUTOR_ROOT_PATH,"");
        if(executorInfo.isEmpty()){
            return;
        }
        try{
            ZookeeperConstant.onlineExecutorSet.add(executorInfo);
            for(int i=0;i<=maxPoolSize;i++){
                idleExecutorThreadSet.add(executorInfo+"/"+i);
                Stat stat =  zookeeperDriverClient.checkExists().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+executorInfo+"/"+i);
                if(stat == null){
                    zookeeperDriverClient.create().creatingParentsIfNeeded()
                            .withMode(CreateMode.PERSISTENT).forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+executorInfo+"/"+i, "1".getBytes());
                }
            }
        }catch(Exception ex){
            //TODO: 创建失败场景
            log.error("fail to regist the executor");
        }
    }

    @Override
    public void executorDown(ChildData oldData)  {
        try{
            Stat stat = zookeeperDriverClient.checkExists().forPath(oldData.getPath());
            if(stat!=null){
                //重放时，将下线-上线的记录，转换成了，上线->下线，其实执行器已经恢复在线
                return ;
            }
        }catch (Exception ex){
            log.error("check executor is down failed, because of the error =>",ex);
            return;
        }
        // 终端掉线后，主动请求终端检查状态是否健康（每2分钟）（健康则等待其上线，异常则创建KAFKA消费者，观察终端下挂任务是否还在执行）
        //检查是否执行中的任务，未执行的将会被收回
        String executorInfo = oldData.getPath().replace(ZookeeperConstant.EXECUTOR_ROOT_PATH,"");
        boolean allDel = true;
        try{
            for(int i=0;i<=maxPoolSize;i++){
                List<String> taskProcessPath = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+executorInfo+"/"+i);
                log.info("executorInfo = [{}],taskProcessPath=[{}]",executorInfo,taskProcessPath);
                if(taskProcessPath==null||taskProcessPath.size()<=0){
                    //没有执行中的任务则进行节点回收
                    zookeeperDriverClient.delete().guaranteed().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+executorInfo+"/"+i);
                }else{
                    allDel=false;
                }
            }
        }catch (Exception ex){
            //TODO: 任务检查失败场景
        }
        if(!allDel){
            //调用执行器接口，同步检查执行器是否还活着
            try{
                ActuatorHealthDTO actuatorHealthDTO = restTemplate.getForObject("http://"+executorInfo+DataxJobConstant.EXECUTOR_HEALTH_CHECK_URL, ActuatorHealthDTO.class);
                if(!"UP".equals(actuatorHealthDTO.getStatus())||!"UP".equals(actuatorHealthDTO.getComponents().getDataxAgentExecutorPool().getStatus())){
                    allDel = removeExecutorThreadByKafkaMsg(executorInfo,oldData);
                }
            }catch (Exception ex){
                try{
                    allDel = removeExecutorThreadByKafkaMsg(executorInfo,oldData);
                }catch (Exception exception){

                    driverEventList.add(new ZookeeperEventDTO("managerExecutor",CuratorCacheListener.Type.NODE_DELETED,oldData,null,120*1000));
                    return;
                }
            }
        }
        //如果15分钟内无任务日志，则回收执行器信息[包括删除JOB-TASK-执行器信息]，并重新指派任务
        if(allDel){
            try{
                zookeeperDriverClient.delete().guaranteed().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+executorInfo);
                ZookeeperConstant.onlineExecutorSet.remove(executorInfo);
            }catch (Exception ex){
                driverEventList.add(new ZookeeperEventDTO("managerExecutor",CuratorCacheListener.Type.NODE_DELETED,oldData,null,120*1000));
            }

        }

    }

    //如果执行器超时无返回，则启动KAFKA消费者，监控是否有任务日志
    private boolean removeExecutorThreadByKafkaMsg(String executorPath,ChildData oldData) throws Exception{
        boolean allDel = true;
        for(int i=0;i<=maxPoolSize;i++){
            List<String> taskProcessPath = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+executorPath+"/"+i);
            log.info("executorInfo = [{}],taskProcessPath ====> [{}]",executorPath,taskProcessPath);
            if(taskProcessPath!=null&&taskProcessPath.size()>0){
                //如果没有KAFKA记录，则删除
                if(DataxJobConstant.executorKafkaLogs.get(taskProcessPath)!=null){
                    if(System.currentTimeMillis()-12*60*1000>=DataxJobConstant.executorKafkaLogs.get(taskProcessPath).getTimestamp() ){
                        zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(executorPath);
                        String[] jobInfo = taskProcessPath.get(0).split(JOB_TASK_SPLIT_TAG);
                        if(jobInfo.length>=2){
                            distributeTask(jobInfo[0],jobInfo[1]);
                        }
                    }else{
                        allDel=false;
                    }
                }else{
                    //延时队列进行再次触发
                    allDel = false;
                    driverEventList.add(new ZookeeperEventDTO("managerExecutor",CuratorCacheListener.Type.NODE_DELETED,oldData,null,120*1000));
                }

            }
        }
        return allDel;
    }

    @Override
    public DataxDTO checkJob(String jobId) throws Exception {
        String data = new String(zookeeperDriverClient.getData().forPath(JOB_LIST_ROOT_PATH+"/"+jobId));
        if(DataxJobConstant.JOB_FINISH.equals(data)){
            zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(JOB_LIST_ROOT_PATH+"/"+jobId);
            return null;
        }
        log.info("the datax orginal data is = [{}]",data);
        DataxDTO dataxDTO = gson.fromJson(data,DataxDTO.class);
        List<String> taskPaths = zookeeperDriverClient.getChildren().forPath(JOB_LIST_ROOT_PATH+"/"+jobId);
        if(taskPaths!=null&&taskPaths.size()>0){
            for(String taskId:taskPaths){
                List<String> taskExecutorPaths = zookeeperDriverClient.getChildren().forPath(JOB_LIST_ROOT_PATH+"/"+jobId+"/"+taskId);
                if(taskExecutorPaths==null||taskExecutorPaths.size()<=0){
                    waitForExecutorJobTaskSet.add(jobId+JOB_TASK_SPLIT_TAG+taskId);
                    distributeTask(jobId,taskId);
                }
            }
        }else{
            splitJob(jobId,dataxDTO);
        }
        return dataxDTO;
    }

    @Override
    public void splitJob(String jobId,DataxDTO jobDto) throws Exception {
        Stat jobStat = zookeeperDriverClient.checkExists().forPath(JOB_LIST_ROOT_PATH+"/"+jobId);
        if(jobStat==null){
            zookeeperDriverClient.create().forPath(JOB_LIST_ROOT_PATH+"/"+jobId,gson.toJson(jobDto).getBytes());
        }
       List<DataxDTO> dataxDTOS = dataxJobSpiltContextService.splitDataxJob(jobDto.getSplitStrategy().getType(),jobId,jobDto);
       for(int i=0,z=dataxDTOS.size();i<z;i++){
           DataxDTO tasksDto = dataxDTOS.get(i);
           //创建子任务
           zookeeperDriverClient.create().forPath(JOB_LIST_ROOT_PATH+"/"+jobId+"/"+tasksDto.getTaskId(),gson.toJson(tasksDto).getBytes());
           waitForExecutorJobTaskSet.add(jobId+JOB_TASK_SPLIT_TAG+tasksDto.getTaskId());
           driverEventList.add(new ZookeeperEventDTO("distributeTask", CuratorCacheListener.Type.NODE_CREATED,new ChildData(jobId,null,"".getBytes()),new ChildData(tasksDto.getTaskId(),null,"".getBytes()),1*1000));
       }
    }

    @Override
    public void distributeTask(String jobId, String taskId) throws Exception {
        //TODO 分配任务，至到没有空余的执行器线程
        Set tmpSet = new HashSet(idleExecutorThreadSet.size());
        tmpSet.addAll(idleExecutorThreadSet);
       Iterator<String> iterator = tmpSet.iterator();
       while(iterator.hasNext()){
           String idleExecutorThread = iterator.next();
           Stat stat = zookeeperDriverClient.checkExists().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+"/"+idleExecutorThread);
           if(stat!=null){
               List<String> threadJobPaths = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+"/"+idleExecutorThread);
               if(threadJobPaths!=null&&idleExecutorThreadSet.size()>0){
                   idleExecutorThreadSet.remove(idleExecutorThread);
               }else{
                   idleExecutorThreadSet.remove(idleExecutorThread);
                   waitForExecutorJobTaskSet.remove(jobId+ JOB_TASK_SPLIT_TAG+taskId);
                   //分配任务至
                   zookeeperDriverClient.create().forPath(JOB_EXECUTOR_ROOT_PATH+"/"+idleExecutorThread+"/"+jobId+ JOB_TASK_SPLIT_TAG+taskId);
                   zookeeperDriverClient.create().forPath(JOB_LIST_ROOT_PATH+"/"+jobId+"/"+taskId+"/"+idleExecutorThread);
                   break;
               }
           }else{
               idleExecutorThreadSet.remove(idleExecutorThread);
           }
       }
    }

    @Override
    public void distributeTask(String executorPath) throws Exception {
        //TODO 分配任务，找到未分配的任务进行分配
        log.info("====>"+executorPath);
        Set tmpSet = new HashSet(waitForExecutorJobTaskSet.size());
        tmpSet.addAll(waitForExecutorJobTaskSet);
        Iterator<String> iterator = tmpSet.iterator();
        while(iterator.hasNext()){
            String waitForExecutorJobTask = iterator.next();
            String[] jobInfo = waitForExecutorJobTask.split(JOB_TASK_SPLIT_TAG);
            if(jobInfo.length>=2){
                String jobId = jobInfo[0];
                Stat jobStat = zookeeperDriverClient.checkExists().forPath(JOB_LIST_ROOT_PATH+"/"+jobId);
                if(jobStat!=null){
                    String taskId = jobInfo[1];
                    Stat taskStat = zookeeperDriverClient.checkExists().forPath(JOB_LIST_ROOT_PATH+"/"+jobId+"/"+taskId);
                    if(taskStat!=null){
                        List<String> taskThreadPaths = zookeeperDriverClient.getChildren().forPath(JOB_LIST_ROOT_PATH+"/"+jobId+"/"+taskId);
                        if(taskThreadPaths==null||taskThreadPaths.size()<=0){
                            idleExecutorThreadSet.remove(executorPath);
                            waitForExecutorJobTaskSet.remove(waitForExecutorJobTask);
                            zookeeperDriverClient.create().forPath(JOB_EXECUTOR_ROOT_PATH+"/"+executorPath+"/"+jobId+ JOB_TASK_SPLIT_TAG+taskId);
                            zookeeperDriverClient.create().forPath(JOB_LIST_ROOT_PATH+"/"+jobId+"/"+taskId+"/"+executorPath);
                            break;

                        }else{
                            waitForExecutorJobTaskSet.remove(waitForExecutorJobTask);
                        }

                    }else{
                        waitForExecutorJobTaskSet.remove(waitForExecutorJobTask);
                    }
                }else{
                    waitForExecutorJobTaskSet.remove(waitForExecutorJobTask);
                }
            }

        }
    }

    @Override
    public void manageJobExecutorChange(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        if(!STATUS_RUNNING.equals(ZookeeperConstant.driverStatus)){
            //调度器初始化未完成，则不进行相关工作调度
            //将初始化期间的事件信息塞入队列中
            driverEventList.add(new ZookeeperEventDTO("manageJobExecutorChange",type,oldData,data,3*1000));

            return ;
        }
        switch (type.toString()){
            case "NODE_DELETED":
                //判断不是任务执行器节点因执行器节点下线而被回收
                if(oldData.getPath().split("/").length>=5){
                    try{
                        jobExecutorRemoveTask(oldData);
                    }catch (Exception ex){
                        //TODO
                    }
                }
                break;
            default:break;
        }
    }

    @Override
    public void jobExecutorRemoveTask(ChildData oldData) throws Exception {
        log.info(" executor finish task ,taskid = [{}]",oldData.getPath());
        String[] pathInfo = oldData.getPath().split("/");
        if(pathInfo.length<=5){
            log.debug("remove not task node ");
            return;
        }

        String jobInfo = pathInfo[pathInfo.length-1];
        String threadId = pathInfo[pathInfo.length-2];
        String executor = pathInfo[pathInfo.length-3];
        /**区分场景**/
        String[] job = jobInfo.split(JOB_TASK_SPLIT_TAG);
        //无足够的资源做任务
        String info = new String(zookeeperDriverClient.getData().forPath(JOB_LIST_ROOT_PATH+"/"+job[0]+"/"+job[1]));
        if(!DataxJobConstant.TASK_FINISH.equals(info)){
            //回收分配的任务，并重新分配
            zookeeperDriverClient.delete().deletingChildrenIfNeeded().forPath(JOB_LIST_ROOT_PATH+"/"+job[0]+"/"+job[1]+"/"+executor);
            waitForExecutorJobTaskSet.add(jobInfo);
            driverEventList.add(new ZookeeperEventDTO("distributeTask", CuratorCacheListener.Type.NODE_CREATED,new ChildData(job[0],null,"".getBytes()),new ChildData(job[1],null,"".getBytes()),1*1000));
            return ;
        }
        //任务已完成
        idleExecutorThreadSet.add(executor+"/"+threadId);
        try{
                removeJobWhenLastTask(job[0],job[1]);
                distributeTask(JOB_EXECUTOR_ROOT_PATH+"/"+executor+"/"+threadId);

        }catch (Exception ex){
            log.error("removeJobWhenLastTask error =>",ex);
        }
    }

    @Override
    public void removeJobWhenLastTask(String jobId, String taskId) throws Exception {
        Stat jobStat = zookeeperDriverClient.checkExists().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId);
        if(jobStat==null){
            return ;
        }
        List<String> taskPaths = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId);
        Stat stat = zookeeperDriverClient.checkExists().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId+"/"+taskId);
        if(stat!=null){
            zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId+"/"+taskId);
            if(taskPaths!=null&&taskPaths.size()==1){
                checkAndRemoveJob(jobId);
            }
        }else{
            if(taskPaths==null||taskPaths.size()<=0){
                checkAndRemoveJob(jobId);
            }
        }
    }

    @Override
    public void stopListenKafka(int num) {
        try{
            String data = new String( zookeeperDriverClient.getData().forPath(DRIVER_PATH));
            if(!("http://"+appInfoComp.getIpAndPort()).equals(data)){
                dataxLogConsumer.stopListen();
            }
        }catch (Exception ex){
            if(num<=5){
                int tmpNum = num;
                stopListenKafka(tmpNum);
            }
        }
    }

    private void checkAndRemoveJob(String jobId)throws Exception {
        Stat jobStat = zookeeperDriverClient.checkExists().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId);
        if(jobStat!=null){
            zookeeperDriverClient.setData().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId,DataxJobConstant.JOB_FINISH.getBytes());
            zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId);
        }
    }

}
