package ni.danny.dataxagent.driver.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.driver.dto.ExecutorThreadDTO;
import ni.danny.dataxagent.driver.dto.JobTaskDTO;
import ni.danny.dataxagent.driver.dto.event.DriverJobEventDTO;
import ni.danny.dataxagent.driver.enums.DriverJobEventTypeEnum;
import ni.danny.dataxagent.driver.producer.DriverJobEventProducerWithTranslator;
import ni.danny.dataxagent.driver.service.DataxDriverExecutorService;
import ni.danny.dataxagent.driver.service.DataxDriverJobService;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.enums.ExecutorTaskStatusEnum;
import ni.danny.dataxagent.service.DataxAgentService;
import ni.danny.dataxagent.service.ListenService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

import static ni.danny.dataxagent.constant.ZookeeperConstant.*;



@Slf4j
@Service
public class DataxDriverServiceImpl implements DataxDriverService {

    @Autowired
    private AppInfoComp appInfoComp;

    @Autowired
    private DriverJobEventProducerWithTranslator driverJobEventProducerWithTranslator;

    @Autowired
    private CuratorFramework zookeeperDriverClient;

    @Autowired
    private ListenService listenService;

    @Autowired
    private DataxDriverJobService dataxDriverJobService;

    @Autowired
    private DataxDriverExecutorService dataxDriverExecutorService;

    @Autowired
    private DataxAgentService dataxAgentService;


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
        dataxDriverJobService.scanJob();
        dataxDriverExecutorService.scanExecutor();
    }

    @Override
    public DataxDTO createJob(DataxDTO dataxDTO) {
        return null;
    }

    @Override
    public void dispatchTask() {
        JobTaskDTO taskDTO =  ZookeeperConstant.pollWaitExecuteTask();
        ExecutorThreadDTO threadDTO = ZookeeperConstant.pollIdleThread();
        if(taskDTO!=null&&threadDTO!=null){
            try{
                String taskThreadPath = ZookeeperConstant.JOB_LIST_ROOT_PATH+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG
                        +taskDTO.getJobId()+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+taskDTO.getTaskId()
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+threadDTO.getExecutor()
                        +ZookeeperConstant.JOB_TASK_SPLIT_TAG+threadDTO.getThread();
                zookeeperDriverClient.create().withTtl(15*60*1000).withMode(CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL)
                        .forPath(taskThreadPath, ExecutorTaskStatusEnum.INIT.getValue().getBytes());

                String threadTaskPath = ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG
                        +threadDTO.getExecutor()+ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+threadDTO.getThread()
                        +ZookeeperConstant.ZOOKEEPER_PATH_SPLIT_TAG+taskDTO.getJobId()+ZookeeperConstant.JOB_TASK_SPLIT_TAG
                        +taskDTO.getTaskId();
                zookeeperDriverClient.create().withTtl(15*60*1000).withMode(CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL)
                        .forPath(threadTaskPath,ExecutorTaskStatusEnum.INIT.getValue().getBytes());
                dataxAgentService.dispatchTask(taskDTO.getJobId(),taskDTO.getTaskId(),threadDTO.getExecutor(),threadDTO.getThread());

                dispatchEvent(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_DISPATCH,null,0
                        ,null,null,5*1000));
            }catch (Exception ex) {
                addWaitExecuteTask(taskDTO);
                addIdleThread(threadDTO);
                dispatchEvent(new DriverJobEventDTO(DriverJobEventTypeEnum.TASK_DISPATCH,null,0
                        ,null,null,5*1000));
            }
        }else if(taskDTO!=null){
            addWaitExecuteTask(taskDTO);

        }else if(threadDTO!=null){
            addIdleThread(threadDTO);
        }
    }



    @Override
    public void addWaitExecuteTask(JobTaskDTO taskDTO){
        Set tmpTaskSet = new HashSet<JobTaskDTO>();
        tmpTaskSet.add(taskDTO);
        ZookeeperConstant.addWaitExecuteTask(tmpTaskSet);
    }

    @Override
    public void addIdleThread(ExecutorThreadDTO threadDTO){
        Set tmpThreadSet = new HashSet<ExecutorThreadDTO>();
        tmpThreadSet.add(threadDTO);
        ZookeeperConstant.addIdleThread(tmpThreadSet);
    }

    private void dispatchEvent(DriverJobEventDTO dto) {
        if(dto.getRetryNum()<10){
            dto.updateRetry();
            driverJobEventProducerWithTranslator.onData(dto);
        }
    }
}
