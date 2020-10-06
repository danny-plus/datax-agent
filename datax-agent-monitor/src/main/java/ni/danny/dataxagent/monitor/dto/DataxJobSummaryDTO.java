package ni.danny.dataxagent.monitor.dto;

import lombok.Data;

@Data
public class DataxJobSummaryDTO {
    private String jobId;
    private String taskId;
    private String status;
    private String lastTime;
    private String jobName;
}
