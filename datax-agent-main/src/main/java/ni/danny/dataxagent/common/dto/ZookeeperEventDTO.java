package ni.danny.dataxagent.common.dto;

import lombok.Data;
import lombok.ToString;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author danny.Ni
 */
@Data
@ToString
public class ZookeeperEventDTO implements Delayed {
    private String method;
    private CuratorCacheListener.Type type;
    private ChildData oldData;
    private ChildData data;

    private final long delay;
    private final long expire;
    private final long now;

    public ZookeeperEventDTO(String method,CuratorCacheListener.Type type,ChildData oldData,ChildData data,long delay){
        this.method = method;
        this.type = type;
        this.oldData = oldData;
        this.data = data;
        this.delay = delay;
        this.expire = System.currentTimeMillis() + delay;
        this.now = System.currentTimeMillis();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(this.expire - System.currentTimeMillis(),unit);
    }

    @Override
    public int compareTo(Delayed o) {
        return (int)(this.getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
    }
}
