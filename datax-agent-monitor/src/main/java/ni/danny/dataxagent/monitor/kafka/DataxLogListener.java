package ni.danny.dataxagent.monitor.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.monitor.dto.DataxJobSummaryDTO;
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

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(groupId = "datax-agent-monitor",topics = {"datax-log"})
    public void listen(String message){
        //%d{HH:mm:ss.SSS}-%X{DATAX-JOBID}-%X{DATAX-TASKID}-%X{DATAX-STATUS}-%X{SOFA-TraceId}-%msg%n
        String msgInfo[] = message.split("-");
        String time = msgInfo[0];
        String jobId = msgInfo[1];
        String taskId = msgInfo[2];
        String status = msgInfo[3];
        String traceId = msgInfo[4];

        DataxJobSummaryDTO jobSummaryDTO = new DataxJobSummaryDTO();
        jobSummaryDTO.setJobId(jobId);
        jobSummaryDTO.setTaskId(taskId);
        jobSummaryDTO.setStatus(status);
        jobSummaryDTO.setLastTime(time);
        StringBuilder msg = new StringBuilder();
        for(int i=5,z=msgInfo.length;i<z;i++){
            msg.append(msgInfo[i]);
            if(i!=z-1){
                msg.append("-");
            }
        }

        ThreadContext.put("dataxJobId", jobId);
        log.info(message);
        simpMessageSendingOperations.convertAndSend("/job/"+jobId,time+"-"+taskId+"-"+status+"-"+msg.toString());
        try{
            String jobMsg = objectMapper.writeValueAsString(jobSummaryDTO);
            simpMessageSendingOperations.convertAndSend("/allJob",jobMsg);
        }catch (JsonProcessingException jsonProcessingException){
            log.warn("objectMapper bean to json fail,reason=>[{}]",jsonProcessingException.getMessage());
        }

    }

}
