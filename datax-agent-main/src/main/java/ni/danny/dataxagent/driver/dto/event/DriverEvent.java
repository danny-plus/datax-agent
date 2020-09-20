package ni.danny.dataxagent.driver.dto.event;

import lombok.Data;
import lombok.ToString;
import ni.danny.dataxagent.driver.enums.DriverJobEventTypeEnum;

@Data
@ToString
public class DriverEvent {
    private DriverEventDTO dto;
    public void clear(){
        dto = null;
    }
}
