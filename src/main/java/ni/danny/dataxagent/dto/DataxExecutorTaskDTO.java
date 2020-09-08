package ni.danny.dataxagent.dto;

import lombok.Data;
import lombok.ToString;

import static ni.danny.dataxagent.constant.ZookeeperConstant.JOB_TASK_SPLIT_TAG;

@Data
@ToString
public class DataxExecutorTaskDTO {
    private String executor;
    private String thread;
    private String jobId;
    private String taskId;
    private String traceId;
    private String nodeData;
    private String jobTask;
    private int checkTimes;

    public DataxExecutorTaskDTO(String executor, String thread,String jobTask,int checkTimes ){
        this.executor = executor;
        this.thread = thread;
        this.jobTask = jobTask;
        this.checkTimes = checkTimes;
        if(jobTask.split(JOB_TASK_SPLIT_TAG).length == 2){
            String[] job = jobTask.split(JOB_TASK_SPLIT_TAG);
           this.jobId = job[0];
           this.taskId = job[1];
        }
    }
}
