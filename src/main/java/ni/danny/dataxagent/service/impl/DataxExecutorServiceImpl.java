package ni.danny.dataxagent.service.impl;


import com.alipay.common.tracer.core.context.trace.SofaTraceContext;
import com.alipay.common.tracer.core.holder.SofaTraceContextHolder;
import com.alipay.common.tracer.core.span.SofaTracerSpan;
import com.alipay.sofa.boot.util.StringUtils;
import com.alipay.sofa.common.utils.StringUtil;
import com.alipay.sofa.tracer.plugins.springmvc.SpringMvcTracer;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.callback.ExecutorDataxJobCallback;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.DataxJobConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.dto.DataxExecutorTaskDTO;
import ni.danny.dataxagent.dto.ZookeeperEventDTO;
import ni.danny.dataxagent.enums.ExecutorTaskStatusEnum;
import ni.danny.dataxagent.service.DataxAgentService;
import ni.danny.dataxagent.service.DataxExecutorService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.MDC;
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

    @Autowired
    private Gson gson;


    @Value("${datax.excutor.pool.maxPoolSize}")
    private int maxPoolSize;

    @Override
    public void regist()  {
        try{
            zookeeperExecutorClient.create().withMode(CreateMode.EPHEMERAL).forPath(ZookeeperConstant.EXECUTOR_ROOT_PATH+"/"+appInfoComp.getIpAndPort(), ("http://"+appInfoComp.getHostnameAndPort()).getBytes());
            listen();
            init();
        }catch (Exception ex){
            //regist();
        }
    }

    @Override
    public void init() throws Exception{
        if(!STATUS_INIT.equals(ZookeeperConstant.updateExecutorStatus(null,STATUS_INIT))){
            log.error("executor init failed, the executor status update failed");
            return;
        }
        executorEventList.clear();
        //TODO 检查所有存在的任务执行节点，是否存在自己的任务，如果有则重启任务开始执行


        ZookeeperConstant.updateExecutorStatus(STATUS_INIT,STATUS_RUNNING);
    }

    @Override
    public void listen() throws Exception {
        listenService.executorWatchJobExecutor();
    }

    @Override
    public void receiveTask(DataxExecutorTaskDTO dto){
        SpringMvcTracer.getSpringMvcTracerSingleton().serverReceive(null);
        SofaTraceContext sofaTraceContext = SofaTraceContextHolder.getSofaTraceContext();
        SofaTracerSpan sofaTracerSpan = sofaTraceContext.getCurrentSpan();
        dto.setTraceId(sofaTracerSpan.getSofaTracerSpanContext().getTraceId());
        try{
            zookeeperExecutorClient.setData().forPath(JOB_EXECUTOR_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId()+"/"+dto.getJobId()+JOB_TASK_SPLIT_TAG+dto.getTaskId(),dto.getTraceId().getBytes());
        }catch (Exception ex){
            rejectTask(dto);
        }
        log.info("new dispatch job arrive ");
        if(0<DataxJobConstant.executorThreadNum.incrementAndGet()
                &&DataxJobConstant.executorThreadNum.incrementAndGet()<=maxPoolSize){

            sofaTracerSpan.setBaggageItem("DATAX-JOBID",dto.getJobId());
            sofaTracerSpan.setBaggageItem("DATAX-TASKID",dto.getTaskId());
            MDC.remove("DATAX-JOBID");
            MDC.remove("DATAX-TASKID");
            MDC.put("DATAX-JOBID",dto.getJobId());
            MDC.put("DATAX-TASKID",dto.getTaskId());
            try{
            String dataxJson = new String(zookeeperExecutorClient.getData().forPath(JOB_LIST_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId()));
            String jobJsonPath = dataxAgentService.createDataxJobJsonFile(dto.getJobId()+JOB_TASK_SPLIT_TAG+dto.getTaskId(),dataxJson);
            zookeeperExecutorClient.setData().forPath(
                    JOB_LIST_ROOT_PATH+"/"+dto.getJobId()+"/"+ dto.getTaskId()+"/"+dto.getExecutor()+JOB_TASK_SPLIT_TAG+dto.getThread()
                    ,ExecutorTaskStatusEnum.RUNNING.getValue().getBytes());
            DataxExecutorService dataxExecutorService = this;
            dataxAgentService.asyncExecuteDataxJob(dto.getJobId(),Integer.parseInt(dto.getTaskId()),jobJsonPath
                    , () -> dataxExecutorService.finishTask(dto));
            }catch (Exception ex){
                rejectTask(dto);
            } catch (Throwable throwable) {
                log.error("dataxAgent async executor Fail");
                rejectTask(dto);
            }
        }else{
            rejectTask(dto);
        }
    }

    @Override
    public void finishTask(DataxExecutorTaskDTO dto)  {
        log.info("[{}] finish , recycle start ",dto);
        try{
            // 重放时不一致问题
            Stat stat = zookeeperExecutorClient.checkExists().forPath(JOB_EXECUTOR_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId()+"/"+dto.getJobId()+JOB_TASK_SPLIT_TAG+dto.getTaskId());
            if(stat != null){
                String tmpTraceId = new String(zookeeperExecutorClient.getData().forPath(JOB_EXECUTOR_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId()+"/"+dto.getJobId()+JOB_TASK_SPLIT_TAG+dto.getTaskId()));
                if(tmpTraceId.equals(dto.getTraceId())){
                zookeeperExecutorClient.setData().forPath(JOB_LIST_ROOT_PATH+"/"+dto.getJobId()+"/"+ dto.getTaskId()+"/"+dto.getExecutor()+JOB_TASK_SPLIT_TAG+dto.getThread(),ExecutorTaskStatusEnum.FINISH.getValue().getBytes());
                zookeeperExecutorClient.delete().guaranteed().forPath(JOB_EXECUTOR_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId()+"/"+dto.getJobId()+JOB_TASK_SPLIT_TAG+dto.getTaskId());
                }else{
                    log.error("not match traceId, nodeData traceId = [{}], dto = [{}]",tmpTraceId,dto);
                }
            }
            log.info("[{}] finish, recycle end ",dto);
        }catch (Exception ex){
            log.info("[{}] finish, recycle error ",dto);
            executorEventList.add(new ZookeeperEventDTO("finishTask", CuratorCacheListener.Type.NODE_DELETED,
                    new ChildData(JOB_EXECUTOR_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId()+"/"+dto.getJobId()+JOB_TASK_SPLIT_TAG+dto.getTaskId(),null,gson.toJson(dto).getBytes()),null,3*1000));
        }
    }

    @Override
    public void rejectTask(DataxExecutorTaskDTO dto) {
        log.info("[{}] reject , recycle start ",dto);
        try{
            // 重放时不一致问题
            Stat stat = zookeeperExecutorClient.checkExists().forPath(JOB_EXECUTOR_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId()+"/"+dto.getJobId()+JOB_TASK_SPLIT_TAG+dto.getTaskId());
            if(stat != null){
                String tmpTraceId = new String(zookeeperExecutorClient.getData().forPath(JOB_EXECUTOR_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId()+"/"+dto.getJobId()+JOB_TASK_SPLIT_TAG+dto.getTaskId()));
                if(tmpTraceId==null||tmpTraceId.isEmpty()||tmpTraceId.equals(dto.getTraceId())){
                zookeeperExecutorClient.setData().forPath(JOB_LIST_ROOT_PATH+"/"+dto.getJobId()+"/"+ dto.getTaskId()+"/"+dto.getExecutor()+JOB_TASK_SPLIT_TAG+dto.getThread(),ExecutorTaskStatusEnum.REJECT.getValue().getBytes());
                zookeeperExecutorClient.delete().guaranteed().forPath(JOB_EXECUTOR_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId()+"/"+dto.getJobId()+JOB_TASK_SPLIT_TAG+dto.getTaskId());
            }else{
                log.error("not match traceId, nodeData traceId = [{}], dto = [{}]",tmpTraceId,dto);
                }
            }
        log.info("[{}] reject, recycle end ",dto);
        }catch (Exception ex){
            log.info("[{}] reject, recycle error ",dto);
            executorEventList.add(new ZookeeperEventDTO("rejectTask", CuratorCacheListener.Type.NODE_DELETED,
                    new ChildData(JOB_EXECUTOR_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId()+"/"+dto.getJobId()+JOB_TASK_SPLIT_TAG+dto.getTaskId(),null,gson.toJson(dto).getBytes()),null,3*1000));

        }
    }


    @Override
    public void process(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        //只监控 data-agent/job/executor/executorIp:port/threadId/jobId-taskId
        if(!checkEvent(oldData,data)){
            log.info("error task path oldData =[{}], data=[{}]",oldData,data);
            return;
        }
        log.info("executor watch job-excutor event Type = [{}], oldData = [{}], data = [{}] ",type,oldData,data);
        if(!STATUS_RUNNING.equals(executorStatus)){
            executorEventList.add(new ZookeeperEventDTO("process",type,oldData,data,1*1000));
            return;
        }

        DataxExecutorTaskDTO dataxExecutorTaskDTO = null;
        if(data != null){
            dataxExecutorTaskDTO = getDataxExecutorTaskDTOByChildData(data);
        }else if(oldData != null){
            dataxExecutorTaskDTO = getDataxExecutorTaskDTOByChildData(oldData);
        }

        try{
        switch (type.toString()){
            case "NODE_CREATED":
                ExecutorTaskStatusEnum createTaskStatusEnum = checkJobExecutorStatus(dataxExecutorTaskDTO);
                if(ExecutorTaskStatusEnum.INIT.equals(createTaskStatusEnum)){
                    receiveTask(dataxExecutorTaskDTO);
                }else{
                    log.info("createTaskStatusEnum in error status = [{}], dto is = [{}]"
                            ,createTaskStatusEnum,dataxExecutorTaskDTO);
                }
                break;
            case "NODE_DELETED":
                ExecutorTaskStatusEnum delTaskStatusEnum = checkJobExecutorStatus(dataxExecutorTaskDTO);
                switch(delTaskStatusEnum.getValue()){
                    case "FINISH": finishTask(dataxExecutorTaskDTO); break;
                    case "REJECT": rejectTask(dataxExecutorTaskDTO); break;
                    default:log.error("delTaskStatusEnum in error status [{}]",oldData);break;
                }
                break;
            case "NODE_CHANGED":
                ExecutorTaskStatusEnum updateTaskStatusEnum = checkJobExecutorStatus(dataxExecutorTaskDTO);

                break;
            default: //log.info("other child event "+type);
                 break;
        }
        }catch (Exception ex){

        }
    }

    private ExecutorTaskStatusEnum checkJobExecutorStatus(DataxExecutorTaskDTO dataxExecutorTaskDTO) throws Exception{
        //任务是否存在
        if(!checkTaskExist(dataxExecutorTaskDTO)){
            return ExecutorTaskStatusEnum.NOT_EXIST;
        }
        //任务是否属于自己
        if(!checkTaskIsSelf(dataxExecutorTaskDTO)){
            return ExecutorTaskStatusEnum.NOT_SELF;
        }
        //看任务状态
        String jobStatus = new String(zookeeperExecutorClient.getData()
                .forPath(JOB_LIST_ROOT_PATH+"/"+dataxExecutorTaskDTO.getJobId()+"/"+dataxExecutorTaskDTO.getTaskId()+"/"+dataxExecutorTaskDTO.getExecutor()+JOB_TASK_SPLIT_TAG+dataxExecutorTaskDTO.getThread()));

        return ExecutorTaskStatusEnum.getTaskStatusByValue(jobStatus);
    }

    private boolean checkTaskIsSelf(DataxExecutorTaskDTO dto) throws Exception {
        boolean result = true;
        Stat taskExecutorStat = zookeeperExecutorClient.checkExists().forPath(JOB_LIST_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId()+"/"+dto.getExecutor()+JOB_TASK_SPLIT_TAG+dto.getThread());
        if(taskExecutorStat == null){
            result = false;
        }
        return result;
    }

    private boolean checkTaskExist(DataxExecutorTaskDTO dto) throws Exception {
        boolean result = true;
        Stat taskStat = zookeeperExecutorClient.checkExists().forPath(JOB_LIST_ROOT_PATH+"/"+dto.getJobId()+"/"+dto.getTaskId());
        if(taskStat==null){
         result = false;
        }
        return result;
    }

    private boolean checkEvent(ChildData oldData, ChildData data){
        boolean result = false;
        if(data!=null){
            result = checkData(data);
        }else if(oldData!=null){
            result = checkData(oldData);
        }
        return result;
    }

    private boolean checkData(ChildData data){
        boolean result = false;
        if(data.getPath().split("/").length==6){
            if(data.getPath().split("/")[5].split(JOB_TASK_SPLIT_TAG).length==2){
                result = true;
            }
        }
        return result;
    }

    private DataxExecutorTaskDTO getDataxExecutorTaskDTOByChildData(ChildData childData){
        String path = childData.getPath();
        String nodeData = new String(childData.getData());
        DataxExecutorTaskDTO dataxExecutorTaskDTO = new DataxExecutorTaskDTO();
        String[] pathInfo = path.split("/");
        //data-agent/job/executor/executorIp:port/threadId/jobId-taskId

        dataxExecutorTaskDTO.setExecutor(pathInfo[3]);
        dataxExecutorTaskDTO.setThread(pathInfo[4]);
        String jobInfo = pathInfo[5];
        String[] job = jobInfo.split(JOB_TASK_SPLIT_TAG);
        dataxExecutorTaskDTO.setJobId(job[0]);
        dataxExecutorTaskDTO.setTaskId(job[1]);
        dataxExecutorTaskDTO.setNodeData(nodeData);
        return dataxExecutorTaskDTO;
    }

}
