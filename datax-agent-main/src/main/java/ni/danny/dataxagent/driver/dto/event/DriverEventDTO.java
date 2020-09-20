package ni.danny.dataxagent.driver.dto.event;

import lombok.Data;
import ni.danny.dataxagent.driver.enums.DriverJobEventTypeEnum;

import java.util.UUID;

@Data
public class DriverEventDTO {
    private DriverJobEventTypeEnum type;
    private String uuid;
    private Integer retryNum;
    private Long currentTime;
    private Long delay;
    private Long delayTime;

    public DriverEventDTO(DriverJobEventTypeEnum type){
        this.uuid = UUID.randomUUID().toString();
        this.type = type;
        this.delay = 0L;
        this.retryNum = 0;
        this.currentTime = System.currentTimeMillis();
        this.delayTime = this.currentTime + this.delay;
    }


    public DriverEventDTO updateRetry(){
        this.retryNum = retryNum+1;
        return this;
    }

    public DriverEventDTO delay(long deplayTime){
        this.delay = deplayTime;
        this.currentTime = System.currentTimeMillis();
        this.delayTime = this.currentTime + this.delay;
        return this;
    }
    public DriverEventDTO delay(){
        this.delay = (long)this.retryNum*500;
        this.currentTime = System.currentTimeMillis();
        this.delayTime = this.currentTime + this.delay;
        return this;
    }
}
