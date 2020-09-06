package ni.danny.dataxagent.service.impl;


import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.dto.ZookeeperEventDTO;
import ni.danny.dataxagent.service.DataxExecutorService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static ni.danny.dataxagent.constant.ZookeeperConstant.*;

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
        //检查所有存在的任务执行节点，是否存在自己的任务，如果有则重启任务开始执行
        if(!STATUS_INIT.equals(ZookeeperConstant.updateExecutorStatus(null,STATUS_INIT))){
            log.error("executor init failed, the executor status update failed");
            return;
        }
        executorEventList.clear();





        if(STATUS_RUNNING.equals(ZookeeperConstant.updateExecutorStatus(STATUS_INIT,STATUS_RUNNING))){
            ZookeeperEventDTO zookeeperEventDTO = executorEventList.poll();
            switch (zookeeperEventDTO.getMethod()){
                case "process": process(zookeeperEventDTO.getType(),zookeeperEventDTO.getOldData(),zookeeperEventDTO.getData());break;
                default: break;
            }
        }
    }

    @Override
    public void regist() {
        try{
            zookeeperExecutorClient.create().withMode(CreateMode.EPHEMERAL).forPath(ZookeeperConstant.EXECUTOR_ROOT_PATH+"/"+appInfoComp.getHostnameAndPort(), ("http://"+appInfoComp.getHostnameAndPort()).getBytes());
            listenService.executorWatchExecutor();
            init();
        }catch (Exception ex){
            log.error("executor regist failed ==>"+ex);
        }
    }

    @Override
    public void process(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        if(!STATUS_RUNNING.equals(executorStatus)){
            executorEventList.add(new ZookeeperEventDTO("process",type,oldData,data));
            return;
        }

        switch (type.toString()){
            case "NODE_CREATED":
                if(data.getPath().split("/").length>=5)
                createTask(data);
            break;
            default: //log.info("other child event "+type);
                 break;
        }
    }

    @Override
    public void createTask(ChildData data) {
        //TODO 小心重放
        log.info("new dispatch job arrive ");
        //TODO 开始执行任务



    }
}
