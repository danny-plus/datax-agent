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

import javax.annotation.Resource;

@Configuration
public class EventConfig {

    @Bean
    public DriverExecutorEventFactory driverExecutorEventFactory(){
        return new DriverExecutorEventFactory();
    }

    @Bean
    public DriverJobEventFactory driverJobEventFactory(){
        return new DriverJobEventFactory();
    }

    @Bean
    public DriverExecutorEventHandler driverExecutorEventHandler(){
        return new DriverExecutorEventHandler();
    }

    @Bean
    public DriverJobEventHandler driverJobEventHandler(){
        return new DriverJobEventHandler();
    }

    @Bean
    public DriverEventFactory driverEventFactory(){
        return new DriverEventFactory();
    }

    @Bean
    public DriverEventHandler driverEventHandler(){return new DriverEventHandler();}

    @Resource
    private DriverExecutorEventFactory driverExecutorEventFactory;

    @Resource
    private DriverExecutorEventHandler driverExecutorEventHandler;

    @Bean
    public RingBuffer<DriverExecutorEvent> driverExecutorEventRingBuffer(){
        Disruptor<DriverExecutorEvent> disruptor
                = new Disruptor<>(driverExecutorEventFactory,1024*1024, DaemonThreadFactory.INSTANCE
                , ProducerType.MULTI,new BlockingWaitStrategy());
        disruptor.handleEventsWith(driverExecutorEventHandler);
        disruptor.start();
        return disruptor.getRingBuffer();
    }

    @Resource
    private DriverJobEventFactory driverJobEventFactory;

    @Resource
    private DriverJobEventHandler driverJobEventHandler;

    @Bean
    public RingBuffer<DriverJobEvent> driverJobEventRingBuffer(){
        Disruptor<DriverJobEvent> disruptor
                = new Disruptor<>(driverJobEventFactory,1024*1024,DaemonThreadFactory.INSTANCE
                ,ProducerType.MULTI,new BlockingWaitStrategy());
        disruptor.handleEventsWith(driverJobEventHandler);
        disruptor.start();
        return disruptor.getRingBuffer();
    }


    @Resource
    private DriverEventFactory driverEventFactory;

    @Resource
    private DriverEventHandler driverEventHandler;

    @Bean
    public RingBuffer<DriverEvent> driverEventRingBuffer(){
        Disruptor<DriverEvent> disruptor
                = new Disruptor<>(driverEventFactory,1024*1024,DaemonThreadFactory.INSTANCE
                ,ProducerType.MULTI,new BlockingWaitStrategy());
        disruptor.handleEventsWith(driverEventHandler);
        disruptor.start();
        return disruptor.getRingBuffer();
    }

    @Resource
    private RingBuffer<DriverExecutorEvent> driverExecutorEventRingBuffer;

    @Bean
    public DriverExecutorEventProducerWithTranslator driverExecutorEventProducerWithTranslator(){
        return new DriverExecutorEventProducerWithTranslator(driverExecutorEventRingBuffer);
    }

    @Resource
    private RingBuffer<DriverJobEvent> driverJobEventRingBuffer;

    @Bean
    public DriverJobEventProducerWithTranslator driverJobEventProducerWithTranslator(){
        return new DriverJobEventProducerWithTranslator(driverJobEventRingBuffer);
    }


    @Resource
    private RingBuffer<DriverEvent> driverEventRingBuffer;

    @Bean
    public DriverEventProducerWithTranslator driverEventProducerWithTranslator(){
        return new DriverEventProducerWithTranslator(driverEventRingBuffer);
    }
}
