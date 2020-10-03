package ni.danny.dataxagent.service.impl;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.Engine;
import com.alipay.common.tracer.core.context.trace.SofaTraceContext;
import com.alipay.common.tracer.core.holder.SofaTraceContextHolder;
import com.alipay.common.tracer.core.span.SofaTracerSpan;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.callback.ExecutorDataxJobCallback;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.enums.ExecutorTaskStatusEnum;
import ni.danny.dataxagent.enums.exception.DataxAgentExceptionCodeEnum;
import ni.danny.dataxagent.exception.DataxAgentCreateJobException;
import ni.danny.dataxagent.exception.DataxAgentException;
import ni.danny.dataxagent.service.DataxAgentService;
import ni.danny.dataxagent.service.DataxJobSpiltContextService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.joda.time.DateTime;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

@Slf4j
@Service
public class DataxAgentServiceImpl implements DataxAgentService {

    @Value("${datax.home}")
    private String dataxHome;

    @Value("${datax.job.home}")
    private String dataxScriptHome;

    @Autowired
    private Gson gson;

    @Autowired
    private DataxJobSpiltContextService dataxJobSpiltContextService;

    @Autowired
    private CuratorFramework zookeeperExecutorClient;

    @Override
    public String createDataxJobJsonFile(DataxDTO dataxDTO) throws IOException, DataxAgentException {
       return createDataxJobJsonFile(
               dataxDTO.getJobId()+ ZookeeperConstant.JOB_TASK_SPLIT_TAG+dataxDTO.getTaskId()
               ,gson.toJson(dataxDTO));
    }

    @Override
    public String createDataxJobJsonFile(String taskName,String json) throws IOException, DataxAgentException {
        byte[] jsonByte = json.getBytes();
        String fullPath = dataxScriptHome+"/"+new DateTime().toString("yyyyMMdd")+ "/"+taskName+".json";
        if(null!=jsonByte){
            File file =new File(fullPath);
            if(!file.exists()){
                File dir = new File(file.getParent());
                dir.mkdirs();
                file.createNewFile();
            }
            FileOutputStream outputStream = new FileOutputStream(file);
            outputStream.write(jsonByte);
            outputStream.close();
            return fullPath;
        }else{
            throw DataxAgentCreateJobException.create(DataxAgentExceptionCodeEnum.JSON_EMPTY,"taskName =["+taskName+"] json is empty");
        }
    }

    @Override
    @Async("agentExecutor")
    public void asyncExecuteDataxJob(String jobId, int taskId, String jobJsonFilePath, ExecutorDataxJobCallback callback) {
        SofaTraceContext sofaTraceContext = SofaTraceContextHolder.getSofaTraceContext();
        SofaTracerSpan sofaTracerSpan = sofaTraceContext.getCurrentSpan();
        sofaTracerSpan.setBaggageItem("DATAX-JOBID",jobId);
        sofaTracerSpan.setBaggageItem("DATAX-TASKID",taskId+"");
        sofaTracerSpan.setBaggageItem("DATAX-STATUS",ExecutorTaskStatusEnum.START.getValue()+"");
        MDC.remove("DATAX-JOBID");
        MDC.remove("DATAX-TASKID");
        MDC.put("DATAX-JOBID",jobId);
        MDC.put("DATAX-TASKID",taskId+"");
        MDC.remove("DATAX-STATUS");
        MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.START.getValue());
        log.info("job [{}-{}] executor ----start----",jobId,taskId);

        System.setProperty("datax.home",dataxHome);

        String[] dataxArgs = {"-job",jobJsonFilePath,"-mode","standalone"
                ,"-jobid",taskId+""};
        try{
            sofaTracerSpan.setBaggageItem("DATAX-STATUS",ExecutorTaskStatusEnum.RUNNING.getValue()+"");
            MDC.remove("DATAX-STATUS");
            MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.RUNNING.getValue());
            Engine.entry(dataxArgs);
        }catch (Throwable e){
            callback.throwException(e);
        }


        MDC.remove("DATAX-STATUS");
        MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.FINISH.getValue());
        log.info("job finished");
        callback.finishTask();
    }

    @Override
    public List<DataxDTO> splitDataxJob(DataxDTO dataxDTO) {
        MDC.remove("DATAX-JOBID");
        MDC.remove("DATAX-TASKID");
        MDC.put("DATAX-JOBID",dataxDTO.getJobId());
        MDC.put("DATAX-TASKID","");
        MDC.remove("DATAX-STATUS");
        MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.INIT.getValue());
        log.info("START SPLIT JOB");
        try {
            return dataxJobSpiltContextService.splitDataxJob(dataxDTO);
        }catch (DataXException dataXException){
            log.error("SPLIT JOB FAILED, errorCode=>{}, message=>{}, cause=>{}",dataXException.getErrorCode()
                    ,dataXException.getMessage(),dataXException.getCause());
            return null;
        }catch (Exception ex){
            log.error("SPLIT JOB FAILED, message=>{}, cause=>{}",ex.getMessage(),ex.getCause());

            return null;
        }
    }

    @Override
    public void finishJob(String jobId) {
        MDC.remove("DATAX-JOBID");
        MDC.remove("DATAX-TASKID");
        MDC.put("DATAX-JOBID",jobId);
        MDC.put("DATAX-TASKID","");
        MDC.remove("DATAX-STATUS");
        MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.FINISH.getValue());
        log.info("FINISH JOB");
    }

    @Override
    public void rejectJob(String jobId) {
        MDC.remove("DATAX-JOBID");
        MDC.remove("DATAX-TASKID");
        MDC.put("DATAX-JOBID",jobId);
        MDC.put("DATAX-TASKID","");
        MDC.remove("DATAX-STATUS");
        MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.REJECT.getValue());
        log.info("REJECT JOB");
    }

    @Override
    public void finishTask(String jobId,int taskId) {
        MDC.remove("DATAX-JOBID");
        MDC.remove("DATAX-TASKID");
        MDC.put("DATAX-JOBID",jobId);
        MDC.put("DATAX-TASKID",taskId+"");
        MDC.remove("DATAX-STATUS");
        MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.FINISH.getValue());
        log.info("FINISH TASK");
    }

    @Override
    public void rejectTask(String jobId,int taskId) {
        MDC.remove("DATAX-JOBID");
        MDC.remove("DATAX-TASKID");
        MDC.put("DATAX-JOBID",jobId);
        MDC.put("DATAX-TASKID",taskId+"");
        MDC.remove("DATAX-STATUS");
        MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.REJECT.getValue());
        log.info("REJECT TASK");
    }

    @Override
    public void dispatchTask(String jobId, int taskId, String executor, int thread) {

        MDC.remove("DATAX-JOBID");
        MDC.remove("DATAX-TASKID");
        MDC.put("DATAX-JOBID",jobId);
        MDC.put("DATAX-TASKID",taskId+"");
        MDC.remove("DATAX-STATUS");
        MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.INIT.getValue());
        log.info("DISPATCH TASK [{}] [{}]",executor,thread);
    }

    @Override
    public void removeJob(String jobId) {
        MDC.remove("DATAX-JOBID");
        MDC.remove("DATAX-TASKID");
        MDC.put("DATAX-JOBID",jobId);
        MDC.remove("DATAX-STATUS");
        MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.REMOVED.getValue());
        log.info("JOB REMOVED");
    }

    @Override
    public void createJob(DataxDTO dto)throws Exception {
        String jobId = dto.getJobId();
        if(jobId.contains(ZookeeperConstant.JOB_TASK_SPLIT_TAG)){
            throw DataxAgentCreateJobException.create(DataxAgentExceptionCodeEnum.JOBID_CONTAINS_DASH,dto.getJobId());
        }
        Stat jobStat = zookeeperExecutorClient.checkExists().forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+dto.getJobId());
        if(jobStat!=null){
            throw DataxAgentCreateJobException.create(DataxAgentExceptionCodeEnum.REPEAT_JOB,dto.getJobId());
        }

        String dataxJson = gson.toJson(dto);
        zookeeperExecutorClient.create().withMode(CreateMode.PERSISTENT)
                .forPath(ZookeeperConstant.JOB_LIST_ROOT_PATH
                +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+dto.getJobId(),dataxJson.getBytes());
    }

}
