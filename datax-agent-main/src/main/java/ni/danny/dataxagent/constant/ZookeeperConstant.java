package ni.danny.dataxagent.constant;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.driver.dto.ExecutorThreadDTO;
import ni.danny.dataxagent.driver.dto.JobTaskDTO;

import java.util.concurrent.*;


@Slf4j
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
    public final static ConcurrentSkipListSet<JobTaskDTO> waitForExecuteTaskSet = new ConcurrentSkipListSet();

    public final static ConcurrentSkipListSet<ExecutorThreadDTO> idleThreadSet = new ConcurrentSkipListSet();

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
