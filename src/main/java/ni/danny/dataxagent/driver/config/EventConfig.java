package ni.danny.dataxagent.driver.config;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import ni.danny.dataxagent.driver.dto.event.*;
import ni.danny.dataxagent.driver.handler.DriverEventHandler;
import ni.danny.dataxagent.driver.handler.DriverExecutorEventHandler;
import ni.danny.dataxagent.driver.handler.DriverJobEventHandler;
import ni.danny.dataxagent.driver.producer.DriverEventProducerWithTranslator;
import ni.danny.dataxagent.driver.producer.DriverExecutorEventProducerWithTranslator;
import ni.danny.dataxagent.driver.producer.DriverJobEventProducerWithTranslator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
public class EventConfig {

    @Lazy
    @Bean
    public DriverExecutorEventFactory driverExecutorEventFactory(){
        return new DriverExecutorEventFactory();
    }

    @Lazy
    @Bean
    public DriverJobEventFactory driverJobEventFactory(){
        return new DriverJobEventFactory();
    }

    @Lazy
    @Bean
    public DriverExecutorEventHandler driverExecutorEventHandler(){
        return new DriverExecutorEventHandler();
    }

    @Lazy
    @Bean
    public DriverJobEventHandler driverJobEventHandler(){
        return new DriverJobEventHandler();
    }

    @Lazy
    @Bean
    public DriverEventFactory driverEventFactory(){
        return new DriverEventFactory();
    }

    @Lazy
    @Bean
    public DriverEventHandler driverEventHandler(){return new DriverEventHandler();}

    @Autowired
    private DriverExecutorEventFactory driverExecutorEventFactory;

    @Autowired
    private DriverExecutorEventHandler driverExecutorEventHandler;

    @Lazy
    @Bean
    public RingBuffer<DriverExecutorEvent> driverExecutorEventRingBuffer(){
        Disruptor<DriverExecutorEvent> disruptor
                = new Disruptor<>(driverExecutorEventFactory,1024*1024, DaemonThreadFactory.INSTANCE
                , ProducerType.MULTI,new BlockingWaitStrategy());
        disruptor.handleEventsWith(driverExecutorEventHandler);
        disruptor.start();
        return disruptor.getRingBuffer();
    }

    @Autowired
    private DriverJobEventFactory driverJobEventFactory;

    @Autowired
    private DriverJobEventHandler driverJobEventHandler;

    @Lazy
    @Bean
    public RingBuffer<DriverJobEvent> driverJobEventRingBuffer(){
        Disruptor<DriverJobEvent> disruptor
                = new Disruptor<>(driverJobEventFactory,1024*1024,DaemonThreadFactory.INSTANCE
                ,ProducerType.MULTI,new BlockingWaitStrategy());
        disruptor.handleEventsWith(driverJobEventHandler);
        disruptor.start();
        return disruptor.getRingBuffer();
    }


    @Autowired
    private DriverEventFactory driverEventFactory;

    @Autowired
    private DriverEventHandler driverEventHandler;

    @Lazy
    @Bean
    public RingBuffer<DriverEvent> driverEventRingBuffer(){
        Disruptor<DriverEvent> disruptor
                = new Disruptor<>(driverEventFactory,1024*1024,DaemonThreadFactory.INSTANCE
                ,ProducerType.MULTI,new BlockingWaitStrategy());
        disruptor.handleEventsWith(driverEventHandler);
        disruptor.start();
        return disruptor.getRingBuffer();
    }

    @Autowired
    private RingBuffer<DriverExecutorEvent> driverExecutorEventRingBuffer;

    @Lazy
    @Bean
    public DriverExecutorEventProducerWithTranslator driverExecutorEventProducerWithTranslator(){
        return new DriverExecutorEventProducerWithTranslator(driverExecutorEventRingBuffer);
    }

    @Autowired
    private RingBuffer<DriverJobEvent> driverJobEventRingBuffer;

    @Lazy
    @Bean
    public DriverJobEventProducerWithTranslator driverJobEventProducerWithTranslator(){
        return new DriverJobEventProducerWithTranslator(driverJobEventRingBuffer);
    }


    @Autowired
    private RingBuffer<DriverEvent> driverEventRingBuffer;

    @Lazy
    @Bean
    public DriverEventProducerWithTranslator driverEventProducerWithTranslator(){
        return new DriverEventProducerWithTranslator(driverEventRingBuffer);
    }
}
