package ni.danny.dataxagent.driver.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.driver.listener.WatchDriverListener;
import ni.danny.dataxagent.driver.listener.WatchExecutorsListener;
import ni.danny.dataxagent.driver.listener.WatchJobsListener;
import ni.danny.dataxagent.kafka.DataxLogConsumer;
import ni.danny.dataxagent.driver.service.DriverListenService;
import org.apache.curator.framework.recipes.cache.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DriverListenServiceImpl implements DriverListenService {


    @Autowired
    private WatchDriverListener watchDriverListener;

    @Autowired
    private WatchExecutorsListener watchExecutorsListener;

    @Autowired
    private WatchJobsListener watchJobsListener;

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
