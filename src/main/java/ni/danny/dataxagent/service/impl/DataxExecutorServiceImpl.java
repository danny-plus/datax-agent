package ni.danny.dataxagent.service.impl;


import com.alipay.sofa.boot.util.StringUtils;
import com.alipay.sofa.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.callback.ExecutorDataxJobCallback;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.DataxJobConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.dto.ZookeeperEventDTO;
import ni.danny.dataxagent.service.DataxAgentService;
import ni.danny.dataxagent.service.DataxExecutorService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static ni.danny.dataxagent.constant.ZookeeperConstant.*;

@Slf4j
@Service
public class DataxExecutorServiceImpl implements DataxExecutorService {

    @Autowired
    private CuratorFramework zookeeperExecutorClient;

    @Autowired
    private ListenService listenService;

    @Autowired
    private AppInfoComp appInfoComp;

    @Autowired
    private DataxAgentService dataxAgentService;


    @Value("${datax.excutor.pool.maxPoolSize}")
    private int maxPoolSize;

    @Override
    public void init() {
        if(!STATUS_INIT.equals(ZookeeperConstant.updateExecutorStatus(null,STATUS_INIT))){
            log.error("executor init failed, the executor status update failed");
            return;
        }
        executorEventList.clear();
        //TODO 检查所有存在的任务执行节点，是否存在自己的任务，如果有则重启任务开始执行


        ZookeeperConstant.updateExecutorStatus(STATUS_INIT,STATUS_RUNNING);
    }

    @Override
    public void regist() {
        try{
            zookeeperExecutorClient.create().withMode(CreateMode.EPHEMERAL).forPath(ZookeeperConstant.EXECUTOR_ROOT_PATH+"/"+appInfoComp.getHostnameAndPort(), ("http://"+appInfoComp.getHostnameAndPort()).getBytes());
            listenService.executorWatchExecutor();
            init();
        }catch (Exception ex){
            log.error("executor regist failed ==>"+ex);
        }
    }

    @Override
    public void process(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        if(!STATUS_RUNNING.equals(executorStatus)){
            executorEventList.add(new ZookeeperEventDTO("process",type,oldData,data,1*1000));
            return;
        }

        switch (type.toString()){
            case "NODE_CREATED":
                if(data.getPath().split("/").length>=5){
                    createTask(data);
                }
            break;
            default: //log.info("other child event "+type);
                 break;
        }
    }

    @Override
    public void createTask(ChildData data) {
        //TODO 小心重放
        log.info("new dispatch job arrive ");
        try{
            if(0<DataxJobConstant.executorThreadNum.incrementAndGet()&&DataxJobConstant.executorThreadNum.incrementAndGet()<=maxPoolSize){
                String[] info = data.getPath().split("/");
                String jobInfo = info[info.length-1];
                String threadId = info[info.length-2];
                String executor = info[info.length-3];
                if(!jobInfo.contains(JOB_TASK_SPLIT_TAG)){
                 log.error("error jobInfo path ==>",jobInfo);
                    zookeeperExecutorClient.delete().guaranteed().forPath(data.getPath());
                    DataxJobConstant.executorThreadNum.decrementAndGet();
                    return ;
                }
                String jobId = jobInfo.split(JOB_TASK_SPLIT_TAG)[0];
                String taskId = jobInfo.split(JOB_TASK_SPLIT_TAG)[1];
                if(!StringUtil.isNumeric(taskId)){
                    throw new Exception("error taskid");
                }
                //TODO 检查任务是否属于自己：看任务下挂线程，看自己线程下挂任务是否一致
                String taskPath = JOB_LIST_ROOT_PATH+"/"+jobId+"/"+taskId;
                Stat stat = zookeeperExecutorClient.checkExists().forPath(taskPath+"/"+executor+"/"+threadId);

                if(stat == null){
                    log.error("error  jobList info does not match the jobExecutor Info  ==>",jobInfo);
                    zookeeperExecutorClient.delete().guaranteed().forPath(data.getPath());
                    DataxJobConstant.executorThreadNum.decrementAndGet();
                    return ;
                }
                //取任务信息，并在本地生成任务文件
                String dataxJson = new String( zookeeperExecutorClient.getData().forPath(JOB_LIST_ROOT_PATH+"/"+jobId+"/"+taskId));

               String jsonFilePath = dataxAgentService.createDataxJobJsonFile(jobInfo,dataxJson);
                //调用线程执行任务
                dataxAgentService.asyncExecuteDataxJob(jobId, Integer.parseInt(taskId), jsonFilePath, new ExecutorDataxJobCallback() {
                    @Override
                    public void finishTask() {
                        try{
                            zookeeperExecutorClient.setData().forPath(taskPath,DataxJobConstant.TASK_FINISH.getBytes());
                            zookeeperExecutorClient.delete().guaranteed().forPath(data.getPath());
                            DataxJobConstant.executorThreadNum.decrementAndGet();
                        }catch (Exception ignore){}
                    }
                });
            }else{
                zookeeperExecutorClient.delete().guaranteed().forPath(data.getPath());
                DataxJobConstant.executorThreadNum.decrementAndGet();
            }
        }catch (Exception ex){
            try{
                zookeeperExecutorClient.delete().guaranteed().forPath(data.getPath());
                DataxJobConstant.executorThreadNum.decrementAndGet();
            }catch (Exception ignore){}

        } catch (Throwable throwable) {
            try{
                zookeeperExecutorClient.delete().guaranteed().forPath(data.getPath());
                DataxJobConstant.executorThreadNum.decrementAndGet();
            }catch (Exception ignore){}
        }


    }
}
