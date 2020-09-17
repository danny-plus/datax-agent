package ni.danny.dataxagent.driver.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.driver.service.DataxDriverExecutorService;
import ni.danny.dataxagent.driver.service.DataxDriverJobService;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import static ni.danny.dataxagent.constant.ZookeeperConstant.*;



@Slf4j
@Service
public class DataxDriverServiceImpl implements DataxDriverService {

    @Autowired
    private AppInfoComp appInfoComp;

    @Autowired
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    private ListenService listenService;

    @Autowired
    private DataxDriverJobService dataxDriverJobService;

    @Autowired
    private DataxDriverExecutorService dataxDriverExecutorService;


    private String driverInfo(){
        return HTTP_PROTOCOL_TAG+appInfoComp.getIpAndPort();
    }

    /**
     * 注册成为Driver【尝试创建Driver节点，并写入自身信息IP:PORT】
     * 注册成功：本地标记当前Driver信息，标记Driver进入初始化状态，
     *         创建两个延时队列【执行器事件队列，任务事件队列】
     *         并启动事件监听【执行器节点上、下线事件；执行器任务节点完成、拒绝事件；新工作创建事件】，调用初始化方法
     *
     * 注册失败：本地标记当前Driver信息，并监听当前的Driver，若Driver被删除【临时节点，SESSION断开则会被删除】，重试本方法
     *
     */
    @Override
    public void regist() {
        try{
            Stat driverStat = zookeeperDriverClient.checkExists().forPath(DRIVER_PATH);
            if(driverStat==null){
                try{
                    zookeeperDriverClient.create().withMode(CreateMode.EPHEMERAL).forPath(DRIVER_PATH,driverInfo().getBytes());
                    beenDriver();
                }catch (Exception ex){
                    checkFail();
                }
            }else{
                checkFail();
            }
        }catch (Exception ex){
            beenWatcher();
        }

    }

    private void checkFail() throws Exception{
        if(checkDriverIsSelf()){
            beenDriver();
        }else{
            beenWatcher();
        }
    }
    @Override
    public boolean checkDriverIsSelf()throws Exception{
        String driverData = new String(zookeeperDriverClient.getData().forPath(DRIVER_PATH));
        updateDriverName(null,driverData);
        if(driverInfo().equals(driverData)) {
            return true;
        }else{
            return false;
        }
    }

    private void beenDriver(){
        updateDriverName(null,driverInfo());
        listenService.stopWatchDriver();
        listen();
        init();
    }

    private void beenWatcher(){
        stopListen();
        listenService.watchDriver();
    }


    @Override
    public void listen() {
        listenService.driverWatchExecutor();
        listenService.driverWatchJob();
        listenService.driverWatchKafkaMsg();
    }

    @Override
    public void stopListen() {
        listenService.stopDriverWatchExecutor();
        listenService.stopDriverWatchJob();
        listenService.stopDriverWatchKafkaMsg();
    }

    @Override
    public void init() {
        DataxDriverService service = this;
        dataxDriverJobService.scanJob(() -> service.jobScanSuccessCallback(), () -> service.jobScanFailCallback());
        dataxDriverExecutorService.scanExecutor(() -> service.executorScanSuccessCallback(), () -> service.executorScanFailCallback());
    }

    @Override
    public void executorScanSuccessCallback() {
        log.info("executor scan success");
    }

    @Override
    public void executorScanFailCallback() {
        log.info("executor scan failed");
    }

    @Override
    public void jobScanSuccessCallback() {
        log.info("job scan success");
    }

    @Override
    public void jobScanFailCallback() {
        log.info("job scan failed");
    }

    @Override
    public DataxDTO createJob(DataxDTO dataxDTO) {
        return null;
    }
}
