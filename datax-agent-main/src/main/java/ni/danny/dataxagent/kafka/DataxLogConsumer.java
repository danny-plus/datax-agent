package ni.danny.dataxagent.kafka;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.DataxJobConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant;

import ni.danny.dataxagent.common.dto.DataxLogDTO;
import ni.danny.dataxagent.enums.ExecutorTaskStatusEnum;
import org.apache.kafka.common.record.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Slf4j
@Component
@EnableKafka
public class DataxLogConsumer {

    @Autowired
    private ConsumerFactory consumerFactory;

    @Autowired(required = false)
    private KafkaListenerEndpointRegistry registry;

    @Bean
    public ConcurrentKafkaListenerContainerFactory delayContainerFactory(){
        ConcurrentKafkaListenerContainerFactory containerFactory = new ConcurrentKafkaListenerContainerFactory();
        containerFactory.setConsumerFactory(consumerFactory);
        containerFactory.setAutoStartup(false);
        return containerFactory;
    }


    @KafkaListener(id="datax-agent-driver",topics = {"datax-log"},containerFactory = "delayContainerFactory" )
    public void listen(Record record){
         //取jobId-taskId-traceId-status|||
        String data = record.value().toString().trim();
        Long timestamp = record.timestamp();
        String jobInfoStr = data.substring(0,data.indexOf("|||"));
        String[] jobInfo = jobInfoStr.split(ZookeeperConstant.JOB_TASK_SPLIT_TAG);
        if(jobInfo.length == 4){
            String jobId = jobInfo[0];
            String taskId = jobInfo[1];
            String traceId = jobInfo[2];
            String nowStatus = jobInfo[3];

            String jobKey = jobId+ZookeeperConstant.JOB_TASK_SPLIT_TAG+taskId;
            if(DataxJobConstant.executorKafkaLogs.get(jobKey)!=null){
                DataxLogDTO dto = DataxJobConstant.executorKafkaLogs.get(jobKey);
                if(timestamp>=dto.getTimestamp()){
                    if(Arrays.asList(dto.getStatus().getCanNext()).contains(nowStatus)){
                        DataxJobConstant.executorKafkaLogs.remove(dto);
                        dto.setTimestamp(timestamp);
                        dto.setTraceId(traceId);
                        dto.setStatus(ExecutorTaskStatusEnum.getTaskStatusByValue(nowStatus));
                        DataxJobConstant.executorKafkaLogs.put(jobKey,dto);
                    }
                }
            }else{
                DataxLogDTO dto = new DataxLogDTO();
                dto.setTraceId(traceId);
                dto.setTimestamp(timestamp);
                dto.setJobId(jobId);
                dto.setTaskId(taskId);
                dto.setStatus(ExecutorTaskStatusEnum.getTaskStatusByValue(nowStatus));
                DataxJobConstant.executorKafkaLogs.put(jobKey,dto);
            }
        }
     }

     public void startListen(){
        log.info("<=========start listen the datax-log==========>");
        log.info("<=========now datax-agent-driver-listen isRunning [{}]==========>",registry.getListenerContainer("datax-agent-driver").isRunning());
        if(!registry.getListenerContainer("datax-agent-driver").isRunning()){
            registry.getListenerContainer("datax-agent-driver").start();
            log.info("<=========started listen the datax-log==========>");
        }
     }

     public void stopListen(){
        if(registry.getListenerContainer("datax-agent-driver").isRunning()){
           registry.getListenerContainer("datax-agent-driver").stop();
        }
     }
}
