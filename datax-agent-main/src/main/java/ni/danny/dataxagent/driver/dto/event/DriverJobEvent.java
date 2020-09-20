package ni.danny.dataxagent.driver.dto.event;

import lombok.Data;

@Data
public class DriverJobEvent {
    private DriverJobEventDTO dto;
    public void clear(){
        dto = null;
    }
}
