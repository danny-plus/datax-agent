package ni.danny.dataxagent.config;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.service.driver.DataxDriverService;
import ni.danny.dataxagent.service.DataxExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Slf4j
@Configuration
public class ZookeeperSessionConnectionListener{

    @Autowired
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    private CuratorFramework zookeeperExecutorClient;

    @Autowired
    private DataxExecutorService dataxExecutorService;

    @Autowired
    private DataxDriverService dataxDriverService;



    @Bean
    public ConnectionStateListener driverSessionConnectionListener(){
        return (curatorFramework, connectionState) -> {
            ZookeeperConstant.updateDriverName(null,"");
            while(true){
                try{
                    if(zookeeperDriverClient.blockUntilConnected(60, TimeUnit.MINUTES)){
                        log.info("ZK RECONNECTED");
                        dataxDriverService.regist();

                        break;
                    }else{

                    }
                }catch (InterruptedException e){
                    break;
                }
            }
        };
    }

    @Bean
    public ConnectionStateListener executorSessionConnectionListener(){
        return (curatorFramework, connectionState) -> {
            while(true){
                try{
                  //  if(curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut()){

                    if(zookeeperExecutorClient.blockUntilConnected(60,TimeUnit.MINUTES)){
                        log.info("ZK RECONNECTED");
                        dataxExecutorService.regist();
                        break;
                    }else{

                    }
                }catch (InterruptedException e){
                    break;
                }
            }
        };
    }
}
