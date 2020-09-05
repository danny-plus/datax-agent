package ni.danny.dataxagent;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DataxAgentApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataxAgentApplication.class, args);
    }

    @Value("${datax.zookeeper.addressAndPort}")
    private String zookeeperAddressAndPort;

    @Bean
    public CuratorFramework zookeeperDriverClient() {
        return CuratorFrameworkFactory.newClient(zookeeperAddressAndPort, new RetryNTimes(5, 1000));
    }

    @Bean
    public CuratorFramework zookeeperExecutorClient() {
        return CuratorFrameworkFactory.newClient(zookeeperAddressAndPort, new RetryNTimes(5, 1000));
    }

}
