package ni.danny.dataxagent.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import ni.danny.dataxagent.common.dto.datax.JobDTO;

@EqualsAndHashCode
@Data
@ToString
@AllArgsConstructor
public class DataxDTO {
    private JobDTO job;
    private String jobId;
    private Integer taskId;
}
