package ni.danny.dataxagent.driver.dto.event;

import lombok.Data;
import ni.danny.dataxagent.driver.enums.DriverExecutorEventTypeEnum;

@Data
public class DriverExecutorEventDTO {
    private DriverExecutorEventTypeEnum type;
    private String jobTask;
    private String executor;
    private int thread;
}
