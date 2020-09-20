package ni.danny.dataxagent.executor.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.executor.service.ExecutorListenService;
import ni.danny.dataxagent.executor.listener.ExecutorWatchSelfListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * @author bingobing
 */
@Slf4j
@Service
public class ExecutorListenServiceImpl implements ExecutorListenService {

    @Autowired
    private AppInfoComp appInfoComp;

    @Autowired
    private CuratorFramework zookeeperExecutorClient;

    @Autowired
    private ExecutorWatchSelfListener executorWatchSelfListener;

    /**
     * 任务节点监听是否有工作派给自己
     */
    @Async("zookeeperThreadExecutor")
    @Override
    public void executorWatchJobExecutor() {
        CuratorCache pathChildrenCache =CuratorCache.builder(zookeeperExecutorClient, ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH
                +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+appInfoComp.getIpAndPort()).build();
        try{
            pathChildrenCache.start();
        pathChildrenCache.listenable().addListener(executorWatchSelfListener);
        }catch (Exception exception){
            exception.printStackTrace();
        }
    }


}
