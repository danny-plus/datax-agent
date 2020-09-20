package ni.danny.dataxagent.dto;

import lombok.*;
import ni.danny.dataxagent.dto.datax.JobDTO;
import ni.danny.dataxagent.dto.splitStrategy.SplitStrategyDTO;

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
