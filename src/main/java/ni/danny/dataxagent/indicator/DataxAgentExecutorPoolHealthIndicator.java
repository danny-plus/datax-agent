package ni.danny.dataxagent.indicator;

import com.alipay.common.tracer.core.async.TracedExecutorService;
import ni.danny.dataxagent.config.DataxAgentExecutorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

@Component("dataxAgentExecutorPool")
public class DataxAgentExecutorPoolHealthIndicator extends AbstractHealthIndicator {

    @Autowired
    private DataxAgentExecutorService agentExecutor;

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        builder.up();
        builder.withDetail("activeCount",agentExecutor.getThreadPoolTaskExecutor().getActiveCount());
        builder.withDetail("corePoolSize",agentExecutor.getThreadPoolTaskExecutor().getCorePoolSize());
        builder.withDetail("maxPoolSize",agentExecutor.getThreadPoolTaskExecutor().getMaxPoolSize());
        builder.withDetail("poolSize",agentExecutor.getThreadPoolTaskExecutor().getPoolSize());

        builder.build();

    }



}
