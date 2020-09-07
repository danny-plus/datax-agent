package ni.danny.dataxagent.service.impl;

import com.alibaba.datax.core.Engine;
import com.alipay.common.tracer.core.async.TracedExecutorService;
import com.alipay.common.tracer.core.context.trace.SofaTraceContext;
import com.alipay.common.tracer.core.holder.SofaTraceContextHolder;
import com.alipay.common.tracer.core.span.SofaTracerSpan;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.callback.ExecutorDataxJobCallback;
import ni.danny.dataxagent.constant.DataxJobConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.service.DataxAgentService;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

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
    private CuratorFramework zookeeperExecutorClient;

    @Override
    public String createDataxJobJsonFile(DataxDTO dataxDTO) {
       return createDataxJobJsonFile(
               dataxDTO.getJobId()+ ZookeeperConstant.JOB_TASK_SPLIT_TAG+dataxDTO.getTaskId()
               ,gson.toJson(dataxDTO));
    }

    @Override
    public String createDataxJobJsonFile(String taskName,String json) {
        return null;
    }

    @Override
    @Async("agentExecutor")
    public void asyncExecuteDataxJob(String jobId, int taskId, String jobJsonFilePath, ExecutorDataxJobCallback callback) throws Throwable {
        SofaTraceContext sofaTraceContext = SofaTraceContextHolder.getSofaTraceContext();
        SofaTracerSpan sofaTracerSpan = sofaTraceContext.getCurrentSpan();
        sofaTracerSpan.setBaggageItem("DATAX-JOBID",jobId);
        sofaTracerSpan.setBaggageItem("DATAX-TASKID",taskId+"");
        MDC.remove("DATAX-JOBID");
        MDC.remove("DATAX-TASKID");
        MDC.put("DATAX-JOBID",jobId);
        MDC.put("DATAX-TASKID",taskId+"");
        log.info("job [{}-{}] executor ----start----",jobId,taskId);

        System.setProperty("datax.home",dataxHome);

        String[] dataxArgs = {"-job",jobJsonFilePath,"-mode","standalone"
                ,"-jobid",taskId+""};

        Engine.entry(dataxArgs);

        callback.finishTask();
    }
}
