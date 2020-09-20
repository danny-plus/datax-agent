package ni.danny.dataxagent.executor.dto.event;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class ExecutorEvent {
    private ExecutorEventDTO dto;
    public void clear(){this.dto = null;}
}
