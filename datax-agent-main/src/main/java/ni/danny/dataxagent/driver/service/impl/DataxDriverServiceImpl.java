package ni.danny.dataxagent.driver.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.driver.dto.ExecutorThreadDTO;
import ni.danny.dataxagent.driver.dto.JobTaskDTO;
import ni.danny.dataxagent.driver.dto.event.DriverEventDTO;
import ni.danny.dataxagent.driver.dto.event.DriverExecutorEventDTO;
import ni.danny.dataxagent.driver.dto.event.DriverJobEventDTO;
import ni.danny.dataxagent.driver.enums.DriverJobEventTypeEnum;
import ni.danny.dataxagent.driver.producer.DriverEventProducerWithTranslator;
import ni.danny.dataxagent.driver.producer.DriverExecutorEventProducerWithTranslator;
import ni.danny.dataxagent.driver.producer.DriverJobEventProducerWithTranslator;
import ni.danny.dataxagent.driver.service.DataxDriverExecutorService;
import ni.danny.dataxagent.driver.service.DataxDriverJobService;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import ni.danny.dataxagent.enums.ExecutorTaskStatusEnum;
import ni.danny.dataxagent.service.DataxAgentService;
import ni.danny.dataxagent.driver.service.DriverListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import static ni.danny.dataxagent.constant.ZookeeperConstant.*;



/**
 * @author danny_ni
 */
@Slf4j
@Service
public class DataxDriverServiceImpl implements DataxDriverService {

    @Autowired
    private AppInfoComp appInfoComp;

    @Autowired
    private DriverJobEventProducerWithTranslator driverJobEventProducerWithTranslator;

    @Autowired
    private DriverExecutorEventProducerWithTranslator driverExecutorEventProducerWithTranslator;

    @Autowired
    @Lazy
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    private DriverListenService listenService;

    @Autowired
    private DataxDriverJobService dataxDriverJobService;

    @Autowired
    private DataxDriverExecutorService dataxDriverExecutorService;

    @Autowired
    private DataxAgentService dataxAgentService;

    @Autowired
    private DriverEventProducerWithTranslator driverEventProducerWithTranslator;


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
        //TODO 暂时不开启KAFKA任务监听
        //listenService.driverWatchKafkaMsg();
    }

    @Override
    public void stopListen() {
        listenService.stopDriverWatchExecutor();
        listenService.stopDriverWatchJob();
        listenService.stopDriverWatchKafkaMsg();
    }

    @Override
    public void init() {
        dataxDriverJobService.scanJob();
        dataxDriverExecutorService.scanExecutor();
    }

    @Async("driverDispatchTaskThreadExecutor")
    @Override
    public  void dispatchTask(DriverEventDTO dto) {
        log.info("try to dispatch the task ");
        JobTaskDTO taskDTO = null;
        ExecutorThreadDTO threadDTO = null;

        synchronized (this){
            taskDTO =  ZookeeperConstant.waitForExecuteTaskSet.pollFirst();

            threadDTO = ZookeeperConstant.idleThreadSet.pollFirst();
            log.info("taskDTO = [{}], threadDTO = [{}]",taskDTO,threadDTO);
        }

        if(taskDTO!=null&&threadDTO!=null){
            try{
                String taskThreadPath = ZookeeperConstant.JOB_LIST_ROOT_PATH+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG
                        +taskDTO.getJobId()+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+taskDTO.getTaskId()
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+threadDTO.getExecutor()
                        +ZookeeperConstant.JOB_TASK_SPLIT_TAG+threadDTO.getThread();
                zookeeperDriverClient.create().withMode(CreateMode.PERSISTENT)
                        .forPath(taskThreadPath, ExecutorTaskStatusEnum.INIT.getValue().getBytes());

                String threadTaskPath = ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG
                        +threadDTO.getExecutor()+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+threadDTO.getThread()
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+taskDTO.getJobId()+ZookeeperConstant.JOB_TASK_SPLIT_TAG
                        +taskDTO.getTaskId();
                zookeeperDriverClient.create().withMode(CreateMode.PERSISTENT)
                        .forPath(threadTaskPath,ExecutorTaskStatusEnum.INIT.getValue().getBytes());
                dataxAgentService.dispatchTask(taskDTO.getJobId(),taskDTO.getTaskId(),threadDTO.getExecutor(),threadDTO.getThread());

                driverEventProducerWithTranslator.onData(dto.delay(5*1000));
            }catch (Exception ex) {
                synchronized (this){
                    addWaitExecuteTask(taskDTO);
                    addIdleThread(threadDTO);
                }
            }
        }else if(taskDTO!=null){
            driverEventProducerWithTranslator.onData(dto.updateRetry());
            addWaitExecuteTask(taskDTO);

        }else if(threadDTO!=null){
            driverEventProducerWithTranslator.onData(dto.updateRetry());
            addIdleThread(threadDTO);
        }
    }



    @Override
    public void addWaitExecuteTask(JobTaskDTO taskDTO){
        boolean insertResult = ZookeeperConstant.waitForExecuteTaskSet.add(taskDTO.priority(-10));
        log.debug("ZookeeperConstant.waitForExecuteTaskSet size = [{}] ,insertResult = [{}]",ZookeeperConstant.waitForExecuteTaskSet.size(),insertResult);

    }

    @Override
    public void addIdleThread(ExecutorThreadDTO threadDTO){
        boolean insertResult = ZookeeperConstant.idleThreadSet.add(threadDTO.priority(-10));
        log.debug("ZookeeperConstant.idleThreadSet size = [{}] ,insertResult = [{}]",ZookeeperConstant.idleThreadSet.size(),insertResult);
    }

    @Override
    public void dispatchJobEvent(DriverJobEventDTO dto) {
        if(dto.getRetryNum()<10){
            dto.updateRetry();
            driverJobEventProducerWithTranslator.onData(dto);
        }
    }

    @Override
    public void dispatchExecutorEvent(DriverExecutorEventDTO dto) {
        if(dto.getRetryNum()<10){
            dto.updateRetry();
            driverExecutorEventProducerWithTranslator.onData(dto);
        }
    }

    @Override
    public void dispatchEvent(DriverEventDTO dto) {
        if(dto.getRetryNum()<10){
            driverEventProducerWithTranslator.onData(dto.updateRetry());
        }
    }
}
