package ni.danny.dataxagent.dto;

import lombok.Data;
import ni.danny.dataxagent.enums.ExecutorTaskStatusEnum;

@Data
public class DataxLogDTO {
    private String jobId;
    private String taskId;
    private String traceId;
    private ExecutorTaskStatusEnum status;
    private Long timestamp;
}
