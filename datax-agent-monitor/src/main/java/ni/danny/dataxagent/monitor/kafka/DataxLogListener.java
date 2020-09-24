package ni.danny.dataxagent.monitor.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.record.Record;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author danny_ni
 */
@Slf4j
@Component
public class DataxLogListener {

    @KafkaListener(groupId = "datax-agent-monitor", topics = {"datax-log"})
    public void listen(Record record){
        log.info(record.value().toString());
    }

}
