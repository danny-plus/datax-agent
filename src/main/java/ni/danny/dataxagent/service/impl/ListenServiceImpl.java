package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.service.DataxDriverService;
import ni.danny.dataxagent.service.DataxExecutorService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
@Service
public class ListenServiceImpl implements ListenService {

    @Autowired
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    private CuratorFramework zookeeperExecutorClient;

    @Autowired
    private DataxExecutorService dataxExecutorService;

    @Autowired
    private DataxDriverService dataxDriverService;

    @Override
    public void watchDriver(String info) {
        NodeCache nodeCache = new NodeCache(zookeeperDriverClient, ZookeeperConstant.DRIVER_PATH);
        try{
            //调用start方法开始监听
            nodeCache.start();
            //添加NodeCacheListener监听器
            ListenService tmpListenService = this;
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    if(nodeCache.getCurrentData()==null){
                        log.info("driver changed ====> removed ");
                        log.info("sleep random millseconds then try be driver ");
                        Thread.sleep(new Random().nextInt(3*1000));
                        try{
                            zookeeperDriverClient.create().withMode(CreateMode.EPHEMERAL).forPath(ZookeeperConstant.DRIVER_PATH, (info).getBytes());
                            dataxDriverService.init();
                        }catch (Exception ex){
                            tmpListenService.watchDriver(info);
                        }
                    }
                }
            });
        }catch (Exception exception){
            exception.printStackTrace();
        }

    }

    /**
     * 监控所有的执行节点
     */
    @Override
    public void driverWatchExecutor() {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(zookeeperDriverClient,ZookeeperConstant.EXECUTOR_ROOT_PATH, false);
        try{
        pathChildrenCache.start();
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                log.info("driver watch catch executor child change===>"+event.getType()+event.getData()+""+event.getInitialData());
                /**
                 * //TODO 自动创建节点线程
                 *             for(int i=0;i<=3;i++){
                 *                 zookeeperExecutorClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+"/1/"+i, "1".getBytes());
                 *             }
                 */
            }
        });
    }catch (Exception exception){
        exception.printStackTrace();
    }
    }

    /**
     * 任务节点监听是否有工作派给自己
     */
    @Override
    public void executorWatchExecutor() {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(zookeeperExecutorClient,ZookeeperConstant.EXECUTOR_ROOT_PATH+"/1", false);
        try{
            pathChildrenCache.start();
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                log.info("executor child change===>"+event.getType()+event.getData()+""+event.getInitialData());
            }
        });

        }catch (Exception exception){
            exception.printStackTrace();
        }
    }
}
