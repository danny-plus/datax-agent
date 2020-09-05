package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
@Service
public class ListenServiceImpl implements ListenService {

    @Autowired
    private ListenService listenService;

    @Override
    public void watchDriver(CuratorFramework zookeeperClient,String info) {
        NodeCache nodeCache = new NodeCache(zookeeperClient, ZookeeperConstant.DRIVER_PATH);
        try{
            //调用start方法开始监听
            nodeCache.start();
            //添加NodeCacheListener监听器
            nodeCache.getListenable().addListener(new NodeCacheListener() {

                @Override
                public void nodeChanged() throws Exception {
                    if(nodeCache.getCurrentData()==null){
                        log.info("driver changed ====> removed ");
                        log.info("sleep random millseconds then try be driver ");
                        Thread.sleep(new Random().nextInt(30*1000));
                        try{
                            zookeeperClient.create().withMode(CreateMode.EPHEMERAL).forPath(ZookeeperConstant.DRIVER_PATH, (info).getBytes());
                        }catch (Exception ex){
                            listenService.watchDriver(zookeeperClient,info);
                        }
                    }

                }
            });
        }catch (Exception exception){
            exception.printStackTrace();
        }

    }


    @Override
    public void watchExecutor(CuratorFramework zookeeperClient) {
    }
}
