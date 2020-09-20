package ni.danny.dataxagent.executor.dto.event;

import lombok.Data;
import lombok.ToString;
import ni.danny.dataxagent.executor.enums.ExecutorEventTypeEnum;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

/**
 * @author bingobing
 */
@Data
@ToString
public class ExecutorEventDTO {
    private ExecutorEventTypeEnum type;
    private CuratorCacheListener.Type nodeEventType;
    private ChildData oldData;
    private ChildData data;
    private Long delay;
    private Long currentTime;
    private Long delayTime;
    private Integer retryNum;

    public ExecutorEventDTO(ExecutorEventTypeEnum type,CuratorCacheListener.Type nodeEventType,ChildData oldData,ChildData data,Long delay){
        this.retryNum=0;
        this.delay = delay;
        this.currentTime = System.currentTimeMillis();
        this.delayTime = this.currentTime+this.delay;
        this.oldData = oldData;
        this.data = data;
        this.nodeEventType = nodeEventType;
        this.type = type;
    }

    public ExecutorEventDTO retry(){
        this.retryNum = retryNum+1;
        return this;
    }

}
