package ni.danny.dataxagent.driver.dto.event;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import ni.danny.dataxagent.driver.enums.DriverExecutorEventTypeEnum;

@Data
@NoArgsConstructor
@ToString
public class DriverExecutorEventDTO {
    private DriverExecutorEventTypeEnum type;
    private String executor;
    private int thread;
    private String jobTask;
    private int retryNum=0;
    public DriverExecutorEventDTO(DriverExecutorEventTypeEnum type,String executor,int thread,String jobTask){
        this.executor = executor;
        this.thread = thread;
        this.type = type;
        this.jobTask = jobTask;
        this.delay = 0;
        this.currentTime = System.currentTimeMillis();
        this.delayTime = this.currentTime + this.delay;
    }

    public DriverExecutorEventDTO(DriverExecutorEventTypeEnum type,String executor,int thread,String jobTask,long delay){
        this.executor = executor;
        this.thread = thread;
        this.type = type;
        this.jobTask = jobTask;
        this.delay = delay;
        this.currentTime = System.currentTimeMillis();
        this.delayTime = this.currentTime + this.delay;
    }
    private long currentTime;
    private long delay;
    private long delayTime;

    public void updateRetry(){
        this.retryNum = retryNum+1;
    }
}
