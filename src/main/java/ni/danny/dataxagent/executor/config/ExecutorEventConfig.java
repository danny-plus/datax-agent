package ni.danny.dataxagent.executor.config;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import ni.danny.dataxagent.driver.dto.event.DriverExecutorEvent;
import ni.danny.dataxagent.executor.dto.event.ExecutorEvent;
import ni.danny.dataxagent.executor.dto.event.ExecutorEventFactory;
import ni.danny.dataxagent.executor.handler.ExecutorEventHandler;
import ni.danny.dataxagent.executor.producer.ExecutorEventProducerWithTranslator;
import org.checkerframework.checker.fenum.qual.AwtFlowLayout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * @author bingobing
 */
@Configuration
public class ExecutorEventConfig {

    @Bean
    @Lazy
    public ExecutorEventFactory executorEventFactory(){
        return new ExecutorEventFactory();
    }

    @Bean
    @Lazy
    public ExecutorEventHandler executorEventHandler(){
        return new ExecutorEventHandler();
    }

    @Autowired
    private ExecutorEventFactory executorEventFactory;

    @Autowired
    private ExecutorEventHandler executorEventHandler;

    @Bean
    @Lazy
    public RingBuffer<ExecutorEvent> executorEventRingBuffer(){
        Disruptor<ExecutorEvent> disruptor
                = new Disruptor<>(executorEventFactory,1024*1024, DaemonThreadFactory.INSTANCE
                , ProducerType.MULTI,new BlockingWaitStrategy());
        disruptor.handleEventsWith(executorEventHandler);
        disruptor.start();
        return disruptor.getRingBuffer();
    }

    @Autowired
    private RingBuffer<ExecutorEvent> executorEventRingBuffer;

    @Bean
    @Lazy
    public ExecutorEventProducerWithTranslator executorEventProducerWithTranslator(){
        return new ExecutorEventProducerWithTranslator(executorEventRingBuffer);
    }


}
