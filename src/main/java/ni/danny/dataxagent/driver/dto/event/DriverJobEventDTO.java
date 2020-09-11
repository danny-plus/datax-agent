package ni.danny.dataxagent.driver.dto.event;

import lombok.Data;
import ni.danny.dataxagent.driver.enums.DriverJobEventTypeEnum;

@Data
public class DriverJobEventDTO {
    private DriverJobEventTypeEnum type;
    private String jobId;
    private int taskId;
    private String exeThreadInfo;

}
