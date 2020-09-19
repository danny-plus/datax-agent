package ni.danny.dataxagent.driver.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Comparator;
import java.util.Objects;

@Data
@ToString
public class JobTaskDTO implements Comparable<JobTaskDTO>{
    private String id;
    private String jobId;
    private int taskId;

    public JobTaskDTO(String jobId,int taskId){
        this.jobId = jobId;
        this.taskId = taskId;
        this.id = this.jobId+"-"+this.taskId;
        this.priority = this.id.hashCode();
    }
    private int priority;

    public JobTaskDTO priority(int priority){
        this.priority = this.id.hashCode()+priority;
        return this;
    }

    @Override
    public int compareTo(JobTaskDTO o) {
        return priority-o.priority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobTaskDTO that = (JobTaskDTO) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
