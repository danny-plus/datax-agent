package ni.danny.dataxagent.config;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import javax.annotation.Resource;

/**
 * @author danny_ni
 */
@Slf4j
@Configuration
public class ZookeeperConfig {

    @Value("${datax.zookeeper.addressAndPort}")
    private String zookeeperAddressAndPort;

    @Bean
    public CuratorFramework zookeeperDriverClient() {
        return CuratorFrameworkFactory
                .builder()
                .connectString(zookeeperAddressAndPort)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new RetryNTimes(5, 1000))
                .namespace(ZookeeperConstant.NAME_SPACE)
                .build();
    }

    @Bean
    public CuratorFramework zookeeperExecutorClient() {
        return CuratorFrameworkFactory
                .builder()
                .connectString(zookeeperAddressAndPort)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new RetryNTimes(5, 1000))
                .namespace(ZookeeperConstant.NAME_SPACE)
                .build();
    }

    @Resource
    @Lazy
    private CuratorFramework zookeeperDriverClient;

    @Bean
    public CuratorCache driverNodeCache(){
        return CuratorCache.builder(zookeeperDriverClient, ZookeeperConstant.DRIVER_PATH).build();
    }

    @Bean
    public CuratorCache executorChildrenCache(){
       return CuratorCache.builder(zookeeperDriverClient, ZookeeperConstant.EXECUTOR_ROOT_PATH).build();
    }

    @Bean
    public CuratorCache jobChildrenCache(){
       return CuratorCache.builder(zookeeperDriverClient, ZookeeperConstant.JOB_ROOT_PATH).build();
    }


}
