package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.dto.DataxExecutorTaskDTO;
import ni.danny.dataxagent.dto.ZookeeperEventDTO;
import ni.danny.dataxagent.kafka.DataxLogConsumer;
import ni.danny.dataxagent.service.DataxDriverService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ni.danny.dataxagent.constant.ZookeeperConstant.*;
import ni.danny.dataxagent.constant.DataxJobConstant.*;

import java.util.List;

import static ni.danny.dataxagent.constant.ZookeeperConstant.*;

@Slf4j
@Service
public class DataxDriverServiceImpl implements DataxDriverService {

    @Autowired
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    private ListenService listenService;

    @Autowired
    private AppInfoComp appInfoComp;

    @Autowired
    private DataxLogConsumer dataxLogConsumer;

    private String driverInfo(){
        return "http://"+appInfoComp.getIpAndPort();
    }

    @Override
    public void regist() {
        try{
            Stat driverStat = zookeeperDriverClient.checkExists().forPath(DRIVER_PATH);
            if(driverStat == null ){
                try{
                    zookeeperDriverClient.create().forPath(DRIVER_PATH,driverInfo().getBytes());
                    beDriver();
                }catch (Exception ex){
                    if(checkDriverIsSelf()){
                        beDriver();
                    }else{
                        notBeDriver();
                    }
                }
            }else{
                if(checkDriverIsSelf()){
                    beDriver();
                }else{
                    notBeDriver();
                }
            }
        }catch (Exception ignore){
            notBeDriver();
        }
    }

    private boolean checkDriverIsSelf() throws Exception{
        String driverData = new String(zookeeperDriverClient.getData().forPath(DRIVER_PATH));
        updateDriverName(null,driverData);
        if(driverInfo().equals(driverData)) {
            return true;
        }else{
            return false;
        }
    }

    private void beDriver(){
        updateDriverStatus(null,STATUS_INIT);
        updateDriverName(null,driverInfo());
        listen();
        init();
    }

    private void notBeDriver(){
        updateDriverStatus(null,STATUS_WATCH);
        listenService.watchDriver();
    }

    @Override
    public void listen() {
        listenService.driverWatchExecutor();
        listenService.driverWatchJobExecutor();
        listenService.driverWatchKafkaMsg();
    }

    @Override
    public void init() {
        //巡检执行器
        scanExecutor();
        //巡检任务
        scanJob();
        updateDriverStatus(STATUS_INIT,STATUS_RUNNING);
    }

    @Override
    public void scanExecutor() {

        //每10分钟进行一次巡检
        driverEventList.add(new ZookeeperEventDTO("scanExecutor",null,null,null,10*60*1000));
    }

    @Override
    public void scanJob() {

        //10分钟进行一次巡检
        driverEventList.add(new ZookeeperEventDTO("scanJob",null,null,null,10*60*1000));
    }

    @Override
    public DataxDTO createJob(DataxDTO dataxDTO) {
        return null;
    }

    @Override
    public List<DataxDTO> splitJob(DataxDTO dataxDTO) {
        return null;
    }

    @Override
    public void dispatchByExecutorThread(String executor, String thread) {

    }

    @Override
    public void dispatchByJobTask(String jobId, String taskId) {

    }

    @Override
    public void dispatchExecutorEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {

    }

    @Override
    public void executorUpEvent(ChildData data) {

    }

    @Override
    public void executorDownEvent(ChildData oldData) {

    }

    @Override
    public void dispatchJobExecutorEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {

    }

    @Override
    public void executorThreadRemoveTask(DataxExecutorTaskDTO dto) {

    }

    @Override
    public void stopListenDataxLog() {
        try{
            dataxLogConsumer.stopListen();
        }catch (Exception ignore){}
    }
}
