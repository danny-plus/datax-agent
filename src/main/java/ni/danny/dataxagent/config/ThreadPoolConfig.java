package ni.danny.dataxagent.config;

import com.alipay.common.tracer.core.async.TracedExecutorService;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableAsync
public class ThreadPoolConfig {

    /** 核心线程数（默认线程数） */
    @Value("${datax.excutor.pool.corePoolSize}")
    private int corePoolSize;
    /** 最大线程数 */
    @Value("${datax.excutor.pool.maxPoolSize}")
    private int maxPoolSize;
    /** 允许线程空闲时间（单位：默认为秒） */
    private static final int keepAliveTime = 10;
    /** 缓冲队列大小 */
    private static final int queueCapacity = 0;
    /** 线程池名前缀 */
    private static final String threadNamePrefix = "datax-agent-";

    @Bean("agentExecutor")
    public TracedExecutorService agentExecutor(){
        ExecutorService executor = Executors.newFixedThreadPool(corePoolSize);
//        executor.setCorePoolSize(corePoolSize);
//        executor.setMaxPoolSize(maxPoolSize);
//        executor.setQueueCapacity(queueCapacity);
//        executor.setKeepAliveSeconds(keepAliveTime);
//        executor.setThreadNamePrefix(threadNamePrefix);
//
//        // 线程池对拒绝任务的处理策略
//        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
//        executor.initialize();

        TracedExecutorService service = new TracedExecutorService(executor);
        return service;
    }

}