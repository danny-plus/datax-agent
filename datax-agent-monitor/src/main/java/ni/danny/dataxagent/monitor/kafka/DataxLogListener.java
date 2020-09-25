package ni.danny.dataxagent.monitor.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.record.Record;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.stereotype.Component;

/**
 * @author danny_ni
 */
@Slf4j
@Component
public class DataxLogListener {

    @Autowired
    private SimpMessageSendingOperations simpMessageSendingOperations;

    @KafkaListener(groupId = "datax-agent-monitor",topics = {"datax-log"})
    public void listen(String message){
        String msgInfo[] = message.split("-");
        String jobId = msgInfo[0];
        String taskId = msgInfo[1];
        String status = msgInfo[2];
        String traceId = msgInfo[3];
        String time = msgInfo[4];
        StringBuilder msg = new StringBuilder();
        for(int i=5,z=msgInfo.length;i<z;i++){
            msg.append(msgInfo[i]);
        }
        ThreadContext.put("dataxJobId", jobId);
        log.info(message);
        simpMessageSendingOperations.convertAndSend("/topic/"+jobId,time+"-"+taskId+"-"+status+"-"+msg.toString());

    }

}
