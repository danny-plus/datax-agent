package ni.danny.dataxagent.service.impl;


import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.service.DataxExecutorService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
@Service
public class DataxExecutorServiceImpl implements DataxExecutorService {

    @Autowired
    private CuratorFramework zookeeperExecutorClient;

    @Autowired
    private ListenService listenService;

    @Autowired
    private AppInfoComp appInfoComp;

    @Override
    public void init() {

    }

    @Override
    public void regist() {
        try{
            zookeeperExecutorClient.create().withMode(CreateMode.EPHEMERAL).forPath(ZookeeperConstant.EXECUTOR_ROOT_PATH+"/"+appInfoComp.getHostnameAndPort(), ("http://"+appInfoComp.getHostnameAndPort()).getBytes());
            init();
            listenService.executorWatchExecutor();
        }catch (Exception ex){
            /**
             * 检查节点是否存在，节点ID是否为自己
             */

            log.error("executor regist failed ==>"+ex);
        }
    }
}
