package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.service.DataxDriverService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
@Service
public class DataxDriverServiceImpl implements DataxDriverService {

    @Autowired
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    private ListenService listenService;

    @Autowired
    private AppInfoComp appInfoComp;



    @Value("${datax.excutor.pool.maxPoolSize}")
    private int maxPoolSize;

    @Override
    public void init() {
        
    }

    @Override
    public void regist() {
        try{
            zookeeperDriverClient.create().withMode(CreateMode.EPHEMERAL).forPath(ZookeeperConstant.DRIVER_PATH, ("http://"+appInfoComp.getHostnameAndPort()).getBytes());
            init();
            listenService.driverWatchExecutor();
        }catch (Exception ex){
            /**
             * TODO: 检查节点是否创建，节点信息是否为自己的
             */

            listenService.watchDriver();
        }
    }

    @Override
    public void managerExecutor(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        switch (type.toString()){
            case "NODE_CREATED":executorUp(data); break;
            case "NODE_DELETED":executorDown(oldData);break;
            default: log.info("other child event "+type);break;
        }
    }

    @Override
    public void executorUp(ChildData data) {
        String executorInfo = data.getPath().replace(ZookeeperConstant.EXECUTOR_ROOT_PATH,"");
        try{
            for(int i=0;i<=maxPoolSize;i++){
                zookeeperDriverClient.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT).forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+executorInfo+"/"+i, "1".getBytes());
            }
        }catch(Exception ex){
            //TODO: 创建失败场景
        }


    }

    @Override
    public void executorDown(ChildData oldData) {
        // 终端掉线后，主动请求终端检查状态是否健康（每2分钟）（健康则等待其上线，异常则创建KAFKA消费者，观察终端下挂任务是否还在执行）

    }
}
