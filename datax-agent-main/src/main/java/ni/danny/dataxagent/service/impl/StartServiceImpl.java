package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.executor.service.DataxExecutorService;
import ni.danny.dataxagent.service.*;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class StartServiceImpl implements StartService {

    @Value("${datax.agent.role}")
    private String agentRole;

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
        if(agentRole.contains("Driver")){
            registerDriver();
        }
        if(agentRole.contains("Executor")){
            registerExecutor();
        }
    }

    @Override
    public void registerDriver() {
        if(!CuratorFrameworkState.STARTED.equals(zookeeperDriverClient.getState())){
            zookeeperDriverClient.start();
        }
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

        if(!CuratorFrameworkState.STARTED.equals(zookeeperExecutorClient.getState())){
            zookeeperExecutorClient.start();
        }
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
