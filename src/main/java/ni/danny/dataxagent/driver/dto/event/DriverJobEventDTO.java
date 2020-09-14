package ni.danny.dataxagent.driver.dto.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ni.danny.dataxagent.driver.enums.DriverJobEventTypeEnum;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DriverJobEventDTO {
    private DriverJobEventTypeEnum type;
    private String jobId;
    private int taskId;
    private String exeThreadInfo;

}
