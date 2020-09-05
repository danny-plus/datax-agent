package ni.danny.dataxagent.service;

import org.apache.curator.framework.CuratorFramework;

public interface ListenService {
    void watchDriver(CuratorFramework zookeeperClient,String info);

    void watchExecutor(CuratorFramework zookeeperClient);

}
