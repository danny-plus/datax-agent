package ni.danny.dataxagent.service.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.DataxJobConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.dto.ZookeeperEventDTO;
import ni.danny.dataxagent.kafka.DataxLogConsumer;
import ni.danny.dataxagent.service.DataxDriverService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    private Gson gson;

    @Autowired
    private DataxLogConsumer dataxLogConsumer;


    @Value("${datax.excutor.pool.maxPoolSize}")
    private int maxPoolSize;

    @Override
    public void init() {
        if(!STATUS_INIT.equals(ZookeeperConstant.updateDriverStatus(null,STATUS_INIT))){
            log.error("driver init failed, the DRIVER STATUS is wrong");
            return ;
        }
        driverEventList.clear();
        //扫描获取所有JOB及其信息，在本地维护一个HASHMAP
        DataxJobConstant.dataxDTOS.clear();

        //TODO: 扫描执行器,并伪造执行器事件




        try{
            List<String> jobPaths = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH);
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
            //重放任务
            log.info("driver init finish, start to replay the change, size=[{}]",driverEventList.size());
           ZookeeperEventDTO zookeeperEventDTO = driverEventList.poll();
           switch (zookeeperEventDTO.getMethod()){
               case "manageJobExecutorChange": manageJobExecutorChange(zookeeperEventDTO.getType(),zookeeperEventDTO.getOldData(),zookeeperEventDTO.getData());break;
               case "managerExecutor": managerExecutor(zookeeperEventDTO.getType(),zookeeperEventDTO.getOldData(),zookeeperEventDTO.getData());break;
               default:break;
           }

        }
    }


    @Override
    public void regist() {
        try{
            zookeeperDriverClient.create().withMode(CreateMode.EPHEMERAL).forPath(ZookeeperConstant.DRIVER_PATH, ("http://"+appInfoComp.getHostnameAndPort()).getBytes());
            listenService.driverWatchExecutor();
            listenService.driverWatchJobExecutor();
            listenService.driverWatchKafkaMsg();
            init();
        }catch (Exception ex){
            try{
                Stat stat =  zookeeperDriverClient.checkExists().forPath(ZookeeperConstant.DRIVER_PATH);
                if(stat == null){
                    regist();
                }else{
                    String info =  new String(zookeeperDriverClient.getData().forPath(ZookeeperConstant.DRIVER_PATH));
                    if(("http://"+appInfoComp.getHostnameAndPort()).equals(info)){
                        listenService.driverWatchExecutor();
                        listenService.driverWatchJobExecutor();
                        listenService.driverWatchKafkaMsg();
                        init();
                    }else{
                        stopListenKafka();
                        listenService.watchDriver();
                    }
                }

            }catch (Exception ignore){}
            stopListenKafka();
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
            driverEventList.add(new ZookeeperEventDTO("managerExecutor",type,oldData,data));
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
            for(int i=0;i<=maxPoolSize;i++){
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
    public void executorDown(ChildData oldData) {
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
            //TODO
            //调用执行器接口，同步检查执行器是否还活着

            //如果执行器超时无返回，则启动KAFKA消费者，监控是否有任务日志

            //如果15分钟内无任务日志，则回收执行器信息[包括删除JOB-TASK-执行器信息]，并重新指派任务

        }
    }

    @Override
    public DataxDTO checkJob(String jobId) throws Exception {
        String data = new String(zookeeperDriverClient.getData().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId));
        if(DataxJobConstant.JOB_FINISH.equals(data)){
            zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId);
            return null;
        }
        log.info("the datax orginal data is = [{}]",data);
        DataxDTO dataxDTO = gson.fromJson(data,DataxDTO.class);
        List<String> taskPaths = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId);
        if(taskPaths!=null&&taskPaths.size()>0){
            for(String taskId:taskPaths){
                List<String> taskExecutorPaths = zookeeperDriverClient.getChildren().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId+"/"+taskId);
                if(taskExecutorPaths==null||taskExecutorPaths.size()<=0){
                    distributeTask(jobId,taskId);
                }
            }
        }else{
            splitJob(jobId);
        }
        return dataxDTO;
    }

    @Override
    public void splitJob(String jobId) {
        //TODO 根据任务信息对任务进行拆解
    }

    @Override
    public void distributeTask(String jobId, String taskId) {
        //TODO 分配任务，至到没有空余的执行器线程


    }

    @Override
    public void distributeTask(String executorPath) {
        //TODO 分配任务，找到未分配的任务进行分配
        log.info("====>"+executorPath);

    }

    @Override
    public void manageJobExecutorChange(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        if(!STATUS_RUNNING.equals(ZookeeperConstant.driverStatus)){
            //调度器初始化未完成，则不进行相关工作调度
            //将初始化期间的事件信息塞入队列中
            driverEventList.add(new ZookeeperEventDTO("manageJobExecutorChange",type,oldData,data));

            return ;
        }
        switch (type.toString()){
            case "NODE_DELETED":
                //判断不是任务执行器节点因执行器节点下线而被回收
                if(oldData.getPath().split("/").length>=5){
                    jobExecutorRemoveTask(oldData);
                }
                break;
            default:break;
        }
    }

    @Override
    public void jobExecutorRemoveTask(ChildData oldData) {
        log.info(" executor finish task ,taskid = [{}]",oldData.getPath());
        String[] pathInfo = oldData.getPath().split("/");
        String jobInfo = pathInfo[pathInfo.length-1];
        String threadId = pathInfo[pathInfo.length-2];
        String executor = pathInfo[pathInfo.length-3];
        try{
            String[] job = jobInfo.split(ZookeeperConstant.JOB_TASK_SPLIT_TAG);
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
    public void stopListenKafka() {
        dataxLogConsumer.stopListen();
    }

    private void checkAndRemoveJob(String jobId)throws Exception {
        Stat jobStat = zookeeperDriverClient.checkExists().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId);
        if(jobStat!=null){
            zookeeperDriverClient.setData().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId,DataxJobConstant.JOB_FINISH.getBytes());
            zookeeperDriverClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH+"/"+jobId);
        }
    }
}
