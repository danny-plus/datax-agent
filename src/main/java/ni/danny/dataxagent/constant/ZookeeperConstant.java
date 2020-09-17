package ni.danny.dataxagent.constant;

import ni.danny.dataxagent.driver.dto.ExecutorThreadDTO;
import ni.danny.dataxagent.driver.dto.JobTaskDTO;
import ni.danny.dataxagent.dto.ZookeeperEventDTO;

import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.*;

public class ZookeeperConstant {
    public final static String NAME_SPACE = "datax-agent";
    public final static String DRIVER_PATH = "/driver";
    public final static String EXECUTOR_ROOT_PATH = "/executor";
    public final static String JOB_ROOT_PATH = "/job";
    /** 任务目录下所有节点均为永久节点 */
    public final static String JOB_EXECUTOR_ROOT_PATH = "/job/executor";

    public final static String JOB_LIST_ROOT_PATH = "/job/list";

    public final static String JOB_TASK_SPLIT_TAG = "-";

    public final static String ZOOKEEPER_PATH_SPLIT_TAG = "/";

    public final static String HTTP_PROTOCOL_TAG = "http://";

    public static String driverName = null ;


    public final static DelayQueue<ZookeeperEventDTO> executorEventList = new DelayQueue<>();

    public static String executorStatus = "INIT";

    public static String STATUS_INIT="INIT";

    public static String STATUS_RUNNING = "RUNNING";

    public static String STATUS_SLEEP = "SLEEP";

    public static String STATUS_WATCH = "WATCH";

    public static String driverJobEventHandlerStatus = STATUS_RUNNING;

    public static String driverExecutorEventHandlerStatus = STATUS_RUNNING;

    public static synchronized void updateDriverExecutorEventHandlerStatus(String newStatus){
        driverExecutorEventHandlerStatus = newStatus;
    }

    public static synchronized void updateDriverJobEventHandlerStatus(String newStatus){
        driverJobEventHandlerStatus = newStatus;
    }

    //使用POLL方法取任务，使用ADD方法添加任务
    public final static PriorityBlockingQueue<JobTaskDTO> waitForExecuteTaskQueue = new PriorityBlockingQueue();
    public final static ConcurrentSkipListSet<String> waitForExecuteTaskSet = new ConcurrentSkipListSet();

    public static void addWaitExecuteTask(Set<JobTaskDTO> dtoSet){
         optWaitExecuteTask("add",dtoSet);
    }
    public static void clearWaitExecuteTask(){
        optWaitExecuteTask("clear",null);
    }

    public static JobTaskDTO pollWaitExecuteTask(){
        return optWaitExecuteTask("poll",null);
    }


    public static synchronized JobTaskDTO optWaitExecuteTask(String opt, Set<JobTaskDTO> dtoSet){
        JobTaskDTO resultDto = null;
        switch (opt){
            case "clear":waitForExecuteTaskSet.clear();waitForExecuteTaskQueue.clear();break;
            case "add":
                if(dtoSet!=null&&!dtoSet.isEmpty()){
                    Iterator<JobTaskDTO> iter = dtoSet.iterator();
                    while(iter.hasNext()) {
                        JobTaskDTO dto = iter.next();
                        if (waitForExecuteTaskSet.add(dto.getJobId() + JOB_TASK_SPLIT_TAG + dto.getTaskId())) {
                            waitForExecuteTaskQueue.add(dto);

                        }
                    }
            }break;
            case "poll":
                try{
                    resultDto = waitForExecuteTaskQueue.poll(100,TimeUnit.MILLISECONDS);
                }catch (InterruptedException ex){
                    resultDto = null;
                }
                if(resultDto!=null){
                    waitForExecuteTaskSet.remove(resultDto.getJobId()+JOB_TASK_SPLIT_TAG+resultDto.getTaskId());
                }
                break;
            default:break;
        }
        return resultDto;
    }

    public final static PriorityBlockingQueue<ExecutorThreadDTO> idleThreadQueue = new PriorityBlockingQueue<>();
    public final static ConcurrentSkipListSet<String> idleThreadSet = new ConcurrentSkipListSet();

    public static void addIdleThread(Set<ExecutorThreadDTO> dtoSet){
        optIdleThread("add",dtoSet);
    }
    public static void clearIdleThread(){
        optIdleThread("clear",null);
    }

    public static ExecutorThreadDTO pollIdleThread(){
        return optIdleThread("poll",null);
    }

    public static synchronized ExecutorThreadDTO optIdleThread(String opt, Set<ExecutorThreadDTO> dtoSet){
        ExecutorThreadDTO dto = null;
        switch (opt){
            case "clear":idleThreadQueue.clear();idleThreadSet.clear(); break;
            case "add":if(dtoSet!=null&&!dtoSet.isEmpty()){
                Iterator<ExecutorThreadDTO> iter = dtoSet.iterator();
                while(iter.hasNext()) {
                    ExecutorThreadDTO tmpDto = iter.next();
                    if (idleThreadSet.add(tmpDto.getExecutor() + JOB_TASK_SPLIT_TAG + dto.getThread())) {
                        idleThreadQueue.add(tmpDto);

                    }
                }
            }break;
            case "poll":
                try{
                    dto = idleThreadQueue.poll(100,TimeUnit.MILLISECONDS);
                }catch (InterruptedException ex){
                    dto = null;
                }
                if(dto!=null){
                    idleThreadSet.remove(dto.getExecutor()+JOB_TASK_SPLIT_TAG+dto.getThread());
                }
                break;
            default:break;
        }
        return dto;
    }

    public static synchronized String updateExecutorStatus(String oldStatus,String newStatus){
        if(oldStatus==null||oldStatus.equals(executorStatus)){
            executorStatus = newStatus;
            return executorStatus;
        }else{
            return executorStatus;
        }
    }

    public static synchronized String updateDriverName(String oldName,String name){
        if(oldName==null || oldName.equals(driverName)){
            driverName = name;
            return driverName;
        }else{
            return driverName;
        }
    }

}
