package ni.danny.dataxagent.driver.dto.event;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import ni.danny.dataxagent.driver.enums.DriverJobEventTypeEnum;

import java.util.UUID;


@Data
@NoArgsConstructor
@ToString
public class DriverJobEventDTO {
    private String id;
    private DriverJobEventTypeEnum type;
    private int retryNum = 0;
    private String jobId;
    private int taskId;
    private String exeThreadInfo;
    private String dataxJson;
    public DriverJobEventDTO(DriverJobEventTypeEnum type,String jobId,int taskId,String exeThreadInfo,String dataxJson){
        this.id = UUID.randomUUID().toString();
        this.type = type;
        this.jobId = jobId;
        this.taskId = taskId;
        this.exeThreadInfo = exeThreadInfo;
        this.dataxJson = dataxJson;
        this.delay = 0;
        this.currentTime = System.currentTimeMillis();
        this.delayTime = this.currentTime + this.delay;
    }
    public DriverJobEventDTO(DriverJobEventTypeEnum type,String jobId,int taskId,String exeThreadInfo,String dataxJson,long delay){
        this.id = UUID.randomUUID().toString();
        this.type = type;
        this.jobId = jobId;
        this.taskId = taskId;
        this.exeThreadInfo = exeThreadInfo;
        this.dataxJson = dataxJson;
        this.delay = delay;
        this.currentTime = System.currentTimeMillis();
        this.delayTime = this.currentTime + this.delay;
    }
    private long currentTime;
    private long delay;
    private long delayTime;

    public DriverJobEventDTO updateRetry(){
        this.id = UUID.randomUUID().toString();
        this.retryNum = this.retryNum+1;
        return this;
    }

}
