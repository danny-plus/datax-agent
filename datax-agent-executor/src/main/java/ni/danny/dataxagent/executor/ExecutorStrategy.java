package ni.danny.dataxagent.executor;

import com.alipay.common.tracer.core.context.trace.SofaTraceContext;
import com.alipay.common.tracer.core.holder.SofaTraceContextHolder;
import com.alipay.common.tracer.core.span.SofaTracerSpan;
import ni.danny.dataxagent.common.dto.DataxDTO;
import ni.danny.dataxagent.common.enums.ExecutorTaskStatusEnum;
import org.slf4j.MDC;

public interface ExecutorStrategy {
    String name();
    void execute(String jobId,Integer taskId,String scriptPath, ExecutorCallback callback);
    default SofaTracerSpan init(String jobId,Integer taskId){
        SofaTraceContext sofaTraceContext = SofaTraceContextHolder.getSofaTraceContext();
        SofaTracerSpan sofaTracerSpan = sofaTraceContext.getCurrentSpan();
        sofaTracerSpan.setBaggageItem("DATAX-JOBID",jobId);
        sofaTracerSpan.setBaggageItem("DATAX-TASKID",taskId+"");
        sofaTracerSpan.setBaggageItem("DATAX-STATUS", ExecutorTaskStatusEnum.START.getValue()+"");
        MDC.remove("DATAX-JOBID");
        MDC.remove("DATAX-TASKID");
        MDC.put("DATAX-JOBID",jobId);
        MDC.put("DATAX-TASKID",taskId+"");
        MDC.remove("DATAX-STATUS");
        MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.START.getValue());
        return sofaTracerSpan;
    }
    default void setRunningStatus(SofaTracerSpan sofaTracerSpan){
        sofaTracerSpan.setBaggageItem("DATAX-STATUS",ExecutorTaskStatusEnum.RUNNING.getValue()+"");
        MDC.remove("DATAX-STATUS");
        MDC.put("DATAX-STATUS", ExecutorTaskStatusEnum.RUNNING.getValue());
    }
}
