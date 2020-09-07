package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.service.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
@Service
public class StartServiceImpl implements StartService {

    @Autowired
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    private CuratorFramework zookeeperExecutorClient;

    @Autowired
    private DataxExecutorService dataxExecutorService;

    @Autowired
    private DataxDriverService dataxDriverService;

    @Autowired
    private ConnectionStateListener driverSessionConnectionListener;

    @Autowired
    private ConnectionStateListener executorSessionConnectionListener;

    @Autowired
    private DriverEventReplayService driverEventReplayService;

    @Autowired
    private ExecutorEventReplayService executorEventReplayService;


    @Override
    public void run(ApplicationArguments applicationArguments) {
        registerDriver();
        registerExecutor();
    }

    @Override
    public void registerDriver() {

        zookeeperDriverClient.start();
        CuratorFrameworkState state = zookeeperDriverClient.getState();
        if(CuratorFrameworkState.STARTED.equals(state)){
            zookeeperDriverClient.getConnectionStateListenable().addListener(driverSessionConnectionListener);
            log.info("zookeeper driver client start");
            driverEventReplayService.replay();
            dataxDriverService.regist();
        }
    }

    @Override
    public void registerExecutor() {
        zookeeperExecutorClient.start();
        CuratorFrameworkState state = zookeeperExecutorClient.getState();
        if(CuratorFrameworkState.STARTED.equals(state)) {
            zookeeperExecutorClient.getConnectionStateListenable().addListener(executorSessionConnectionListener);
            log.info("zookeeper executor client start");
            executorEventReplayService.replay();
            dataxExecutorService.regist();
        }
    }
}
