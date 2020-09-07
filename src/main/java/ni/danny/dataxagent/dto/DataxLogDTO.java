package ni.danny.dataxagent.dto;

import lombok.Data;

@Data
public class DataxLogDTO {
    private String jobId;
    private String taskId;
    private String traceId;
    private Long timestamp;
}
