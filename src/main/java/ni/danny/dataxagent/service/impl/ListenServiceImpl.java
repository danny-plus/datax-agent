package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.driver.listener.WatchDriverListener;
import ni.danny.dataxagent.driver.listener.WatchExecutorsListener;
import ni.danny.dataxagent.driver.listener.WatchJobsListener;
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
    private CuratorFramework zookeeperExecutorClient;

    @Autowired
    private DataxExecutorService dataxExecutorService;

    @Autowired
    private WatchDriverListener watchDriverListener;

    @Autowired
    private WatchExecutorsListener watchExecutorsListener;

    @Autowired
    private WatchJobsListener watchJobsListener;


    @Autowired
    private AppInfoComp appInfoComp;

    @Autowired
    private DataxLogConsumer dataxLogConsumer;

    @Autowired
    private CuratorCache driverNodeCache;

    @Autowired
    private CuratorCache executorChildrenCache;

    @Autowired
    private  CuratorCache jobChildrenCache;

    @Override
    public void watchDriver() {
        try{
            //调用start方法开始监听
            driverNodeCache.start();
            //添加NodeCacheListener监听器
            driverNodeCache.listenable().addListener(watchDriverListener);
        }catch (Exception exception){
            log.error("start driver node cache failed or addListener failed");
        }
    }

    @Override
    public void stopWatchDriver() {
        driverNodeCache.listenable().removeListener(watchDriverListener);
    }

    /**
     * 监控所有的执行节点
     */
    @Override
    public void driverWatchExecutor() {
        try{
            executorChildrenCache.start();
            executorChildrenCache.listenable().addListener(watchExecutorsListener);
    }catch (Exception exception){
            log.error("start executorChildrenCache failed or addListener failed");
    }
    }

    @Override
    public void stopDriverWatchExecutor() {
        executorChildrenCache.listenable().removeListener(watchExecutorsListener);
    }

    @Override
    public void driverWatchJob() {
        try{
            jobChildrenCache.start();
            jobChildrenCache.listenable().addListener(watchJobsListener);
            log.info("driverWatchJob success");
        }catch (Exception exception){
            log.error("start jobChildrenCache failed or addListener failed");
        }

    }

    @Override
    public void stopDriverWatchJob() {
        jobChildrenCache.listenable().removeListener(watchJobsListener);
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

    @Override
    public void stopDriverWatchKafkaMsg() {
        try{
            dataxLogConsumer.stopListen();
        }catch (Exception ignore){}
    }
}
