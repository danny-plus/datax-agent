package ni.danny.dataxagent.executor.strategy;

import com.alibaba.datax.core.Engine;
import com.alipay.common.tracer.core.context.trace.SofaTraceContext;
import com.alipay.common.tracer.core.holder.SofaTraceContextHolder;
import com.alipay.common.tracer.core.span.SofaTracerSpan;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.common.dto.DataxDTO;
import ni.danny.dataxagent.common.enums.ExecutorTaskStatusEnum;
import ni.danny.dataxagent.executor.ExecutorCallback;
import ni.danny.dataxagent.executor.ExecutorStrategy;
import ni.danny.dataxagent.executor.bo.SSHClientBO;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component("shellExecutor")
public class ExecutorByShellImpl implements ExecutorStrategy {
    @Override
    public String name() {
        return "shell";
    }

    @Value("${datax.home}")
    private String dataxHome;

    @Autowired
    private SSHClientBO sshClient;

    @Override
    public void execute(String jobId,Integer taskId,String scriptPath, ExecutorCallback callback) {
        SofaTracerSpan sofaTracerSpan =init(jobId,taskId);
        log.info("job [{}-{}] executor ----start----",jobId,taskId);
        try{
            setRunningStatus(sofaTracerSpan);
            StringBuilder builder = sshClient.exec("python "+dataxHome+"/bin/datax.py "+scriptPath+" -jobid "+taskId);
            log.info(builder.toString());
            callback.success();
        }catch (Throwable e){
            callback.throwException(e);
        }
    }
}
