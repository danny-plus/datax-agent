package ni.danny.dataxagent.common.dto;

import lombok.*;
import ni.danny.dataxagent.common.dto.datax.JobDTO;
import ni.danny.dataxagent.common.dto.splitStrategy.SplitStrategyDTO;

@EqualsAndHashCode
@Data
@ToString
@AllArgsConstructor
public class DataxDTO {
    private JobDTO job;
    private SplitStrategyDTO splitStrategy;
    private String jobId;
    private int taskId;

}
