package ni.danny.dataxagent.service;

import org.apache.curator.framework.CuratorFramework;

public interface ListenService {
    void watchDriver();
    void driverWatchExecutor();
    void executorWatchExecutor();

}
