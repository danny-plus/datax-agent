package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.kafka.DataxLogConsumer;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import ni.danny.dataxagent.service.DataxExecutorService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Random;

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
    private AppInfoComp appInfoComp;

    @Autowired
    private DataxLogConsumer dataxLogConsumer;

    @Override
    public void watchDriver() {
        CuratorCache nodeCache = CuratorCache.builder(zookeeperDriverClient, ZookeeperConstant.DRIVER_PATH).build();
        try{
            //调用start方法开始监听
            nodeCache.start();
            //添加NodeCacheListener监听器

            nodeCache.listenable().addListener((type, oldData, data) -> {
                log.info("driver changed ====> {} ",type);
                if(type== CuratorCacheListener.Type.NODE_DELETED){
                    log.info("sleep random millseconds then try be driver ");
                    try{
                        Thread.sleep(new Random().nextInt(3*1000));
                    }catch (InterruptedException ignore){

                    }
                   //  dataxDriverService.regist();
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
        CuratorCache pathChildrenCache = CuratorCache.builder(zookeeperDriverClient, ZookeeperConstant.EXECUTOR_ROOT_PATH).build();
        try{
        pathChildrenCache.start();
        pathChildrenCache.listenable().addListener(
                (type, oldData, data) -> {
                  //  dataxDriverService.dispatchExecutorEvent(type,oldData,data);
                }
        );
    }catch (Exception exception){
        exception.printStackTrace();
    }
    }

    @Override
    public void driverWatchJobExecutor() {
        CuratorCache pathChildrenCache =CuratorCache.builder(zookeeperExecutorClient, ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH).build();
        try{
            pathChildrenCache.start();
            pathChildrenCache.listenable().addListener((type, oldData, data) -> {
                //dataxDriverService.dispatchJobExecutorEvent(type,oldData,data);
            });
        }catch (Exception exception){
            exception.printStackTrace();
        }

    }

    /**
     * 任务节点监听是否有工作派给自己
     */
    @Override
    public void executorWatchJobExecutor() {
        CuratorCache pathChildrenCache =CuratorCache.builder(zookeeperExecutorClient, ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+"/"+appInfoComp.getHostnameAndPort()).build();
        try{
            pathChildrenCache.start();
        pathChildrenCache.listenable().addListener((type, oldData, data) -> {
            //log.info("executor watch job executor child change===>"+type+"   "+oldData+"   "+data);
            dataxExecutorService.process(type,oldData,data);

        });
        }catch (Exception exception){
            exception.printStackTrace();
        }
    }


    @Override
    public void driverWatchKafkaMsg() {
        dataxLogConsumer.startListen();
    }
}
