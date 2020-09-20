package ni.danny.dataxagent.config;

import com.alipay.common.tracer.core.async.TracedExecutorService;
import com.alipay.common.tracer.core.context.trace.SofaTraceContext;
import lombok.Getter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ExecutorService;

public class DataxAgentExecutorService extends TracedExecutorService {

    @Getter
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    public DataxAgentExecutorService(ThreadPoolTaskExecutor threadPoolTaskExecutor){
        super(threadPoolTaskExecutor.getThreadPoolExecutor());
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    }

    public DataxAgentExecutorService(ExecutorService delegate) {
        super(delegate);
    }

    public DataxAgentExecutorService(ExecutorService delegate, SofaTraceContext traceContext) {
        super(delegate, traceContext);
    }

    public SofaTraceContext getTraceContext(){
        return this.traceContext;
    }

}
