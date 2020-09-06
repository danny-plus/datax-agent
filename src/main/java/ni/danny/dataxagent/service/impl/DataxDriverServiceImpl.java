package ni.danny.dataxagent.service.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.DataxJobConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.service.DataxDriverService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static ni.danny.dataxagent.constant.ZookeeperConstant.DRIVER_STATUS_INIT;
import static ni.danny.dataxagent.constant.ZookeeperConstant.DRIVER_STATUS_RUNNING;

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



    @Value("${datax.excutor.pool.maxPoolSize}")
    private int maxPoolSize;

    @Override
    public void init() {
        if(!DRIVER_STATUS_INIT.equals(ZookeeperConstant.updateDriverStatus(null,DRIVER_STATUS_INIT))){
            log.info("driver init failed, the DRIVER STATUS is wrong");
            return ;
        }
        //扫描获取所有JOB及其信息，在本地维护一个HASHMAP
        DataxJobConstant.dataxDTOS.clear();
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
        ZookeeperConstant.updateDriverStatus(DRIVER_STATUS_INIT,DRIVER_STATUS_RUNNING);
    }


    @Override
    public void regist() {
        try{
            zookeeperDriverClient.create().withMode(CreateMode.EPHEMERAL).forPath(ZookeeperConstant.DRIVER_PATH, ("http://"+appInfoComp.getHostnameAndPort()).getBytes());
            init();
            listenService.driverWatchExecutor();
            listenService.dviverWatchJobExecutor();
        }catch (Exception ex){
            try{
                Stat stat =  zookeeperDriverClient.checkExists().forPath(ZookeeperConstant.DRIVER_PATH);
                if(stat == null){
                    regist();
                }else{
                    String info =  new String(zookeeperDriverClient.getData().forPath(ZookeeperConstant.DRIVER_PATH));
                    if(("http://"+appInfoComp.getHostnameAndPort()).equals(info)){
                        init();
                        listenService.driverWatchExecutor();
                        listenService.dviverWatchJobExecutor();
                    }else{
                        listenService.watchDriver();
                    }
                }

            }catch (Exception ignore){}
            listenService.watchDriver();
        }
    }

    @Override
    public void managerExecutor(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        //调度器初始化未完成，则不进行相关工作调度
        if(!DRIVER_STATUS_RUNNING.equals(ZookeeperConstant.driverStatus)){
            //调度器初始化未完成，则不进行相关工作调度
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



    }

    @Override
    public void manageJobExecutorChange(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        if(!DRIVER_STATUS_RUNNING.equals(ZookeeperConstant.driverStatus)){
            //调度器初始化未完成，则不进行相关工作调度
            return ;
        }
        switch (type.toString()){
            case "NODE_DELETED":
                //判断不是任务执行器节点因执行器节点下线而被回收
                if(oldData.getPath().split("/").length>=5)
                jobExecutorRemoveTask(oldData); break;
            default:break;
        }
    }

    @Override
    public void jobExecutorRemoveTask(ChildData oldData) {
        log.info(" executor finish task ,taskid = [{}]",oldData.getPath());
    }
}
