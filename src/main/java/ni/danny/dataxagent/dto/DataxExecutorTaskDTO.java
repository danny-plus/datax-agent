package ni.danny.dataxagent.dto;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class DataxExecutorTaskDTO {
    private String executor;
    private String thread;
    private String jobId;
    private String taskId;
    private String traceId;
    private String nodeData;
}
