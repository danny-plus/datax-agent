package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.service.*;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

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


    @Override
    public void run(ApplicationArguments applicationArguments) {
        registerDriver();
        registerExecutor();
    }

    @Override
    public void registerDriver() {

        zookeeperDriverClient.start();
        try{
            Stat stat = zookeeperDriverClient.checkExists().forPath(ZookeeperConstant.DRIVER_PATH);
        }catch (Exception ex){
            try{ Thread.sleep(1*1000);
                registerDriver();
            }catch (Exception ignore){}
        }
        for(;;){
            CuratorFrameworkState state = zookeeperDriverClient.getState();
            if(CuratorFrameworkState.STARTED.equals(state)){
                break;
            }
            try{
                Thread.sleep(300);
                registerExecutor();
            }catch (Exception ignore){}
        }
        zookeeperDriverClient.getConnectionStateListenable().addListener(driverSessionConnectionListener);
        log.info("zookeeper driver client start");
        dataxDriverService.regist();
    }

    @Override
    public void registerExecutor() {
        zookeeperExecutorClient.start();
        try{
            Stat stat = zookeeperExecutorClient.checkExists().forPath(ZookeeperConstant.DRIVER_PATH);
        }catch (Exception ex){
            try{ Thread.sleep(1*1000);
                registerExecutor();
            }catch (Exception ignore){}
        }
        for(;;){
            CuratorFrameworkState state = zookeeperExecutorClient.getState();
            if(CuratorFrameworkState.STARTED.equals(state)) {
                break;
            }
            try{
                Thread.sleep(300);
                registerExecutor();
            }catch (Exception ignore){}
        }
        zookeeperExecutorClient.getConnectionStateListenable().addListener(executorSessionConnectionListener);
        log.info("zookeeper executor client start");
        dataxExecutorService.regist();

    }
}
