package ni.danny.dataxagent.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
/**
 * @author danny.Ni
 */
@Data
@AllArgsConstructor
public class ZookeeperEventDTO {
    private String method;
    private CuratorCacheListener.Type type;
    private ChildData oldData;
    private ChildData data;
}
