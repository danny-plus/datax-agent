package ni.danny.dataxagent.driver.dto.event;

import lombok.Data;

@Data
public class DriverExecutorEvent {
    private DriverExecutorEventDTO dto;
    public void clear(){
        dto = null;
    }
}
