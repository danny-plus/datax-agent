package ni.danny.dataxagent.driver.dto;

import lombok.Data;

import java.util.Comparator;

@Data
public class JobTaskDTO implements Comparator<JobTaskDTO>{
    private String jobId;
    private int taskId;

    public JobTaskDTO(String jobId,int taskId){
        this.jobId = jobId;
        this.taskId = taskId;
    }
    private int priority=10;


    @Override
    public int compare(JobTaskDTO o1, JobTaskDTO o2) {
        return o1.priority-o2.priority;
    }
}
