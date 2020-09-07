package ni.danny.dataxagent.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

@Slf4j
@Component("dataxLogConsumer")
public class DataxLogConsumer {

    @Autowired
    private ConsumerFactory consumerFactory;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Bean
    public ConcurrentKafkaListenerContainerFactory delayContainerFactory(){
        ConcurrentKafkaListenerContainerFactory containerFactory = new ConcurrentKafkaListenerContainerFactory();
        containerFactory.setConsumerFactory(consumerFactory);
        containerFactory.setAutoStartup(false);
        return containerFactory;
    }


     @KafkaListener(id="datax-agent-driver",topics = {"${spring.kafka.topic}"},containerFactory = "delayContainerFactory" )
    public void listen(String data){
         log.info("kafka get log ==== "+data);
     }

     public void startListen(){
        if(!registry.getListenerContainer("datax-agent-driver").isRunning()){
            registry.getListenerContainer("datax-agent-driver").start();
        }
     }

     public void stopListen(){
        if(registry.getListenerContainer("datax-agent-driver").isRunning()){
           registry.getListenerContainer("datax-agent-driver").stop();
        }
     }
}
