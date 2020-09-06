package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.service.DataxDriverService;
import ni.danny.dataxagent.service.DataxExecutorService;
import ni.danny.dataxagent.service.ListenService;
import ni.danny.dataxagent.service.StartService;
import org.apache.curator.framework.CuratorFramework;
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


    @Override
    public void run(ApplicationArguments applicationArguments) {
        registerDriver();
        registerExecutor();
    }

    @Override
    public void registerDriver() {
        zookeeperDriverClient.start();
        log.info("zookeeper driver client start");
        dataxDriverService.regist();
    }

    @Override
    public void registerExecutor() {
        zookeeperExecutorClient.start();
        log.info("zookeeper executor client start");
        dataxExecutorService.regist();
    }
}
