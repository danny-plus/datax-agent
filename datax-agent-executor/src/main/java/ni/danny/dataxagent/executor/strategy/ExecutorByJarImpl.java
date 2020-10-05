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
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component("jarExecutor")
public class ExecutorByJarImpl implements ExecutorStrategy {

    @Value("${datax.home}")
    private String dataxHome;

    @Override
    public String name() {
        return "jar";
    }
    @Override
    public void execute(String jobId,Integer taskId,String scriptPath, ExecutorCallback callback) {
        SofaTracerSpan sofaTracerSpan = init(jobId,taskId);
        log.info("job [{}-{}] executor ----start----",jobId,taskId);
        System.setProperty("datax.home",dataxHome);
        String[] dataxArgs = {"-job",scriptPath,"-mode","standalone"
                ,"-jobid",taskId+""};
        try{
            setRunningStatus(sofaTracerSpan);
            Engine.entry(dataxArgs);
            callback.success();
        }catch (Throwable e){
            callback.throwException(e);
        }
    }
}
