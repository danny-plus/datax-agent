package ni.danny.dataxagent.dto.event;

import lombok.Data;

@Data
public class DriverJobEventDTO {
    private String jobId;
    private int taskId;
    private String jobTask;
    private String executor;
    private int thread;
    private String exeThreadInfo;
}
