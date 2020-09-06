package ni.danny.dataxagent.constant;

public class ZookeeperConstant {
    public final static String NAME_SPACE = "datax-agent";
    public final static String DRIVER_PATH = "/driver";
    public final static String EXECUTOR_ROOT_PATH = "/executor";

    /** 任务目录下所有节点均为永久节点 */
    public final static String JOB_EXECUTOR_ROOT_PATH = "/job/executor";

    public final static String JOB_LIST_ROOT_PATH = "/job/list";

    public final static String JOB_TASK_SPLIT_TAG = "^-^";

    public static String driverStatus = "INIT";

    public static String DRIVER_STATUS_INIT="INIT";

    public static String DRIVER_STATUS_RUNNING = "RUNNING";

    public static synchronized String updateDriverStatus(String oldStatus,String newStatus){
        if(oldStatus==null||oldStatus.equals(driverStatus)){
            ZookeeperConstant.driverStatus = newStatus;
            return ZookeeperConstant.driverStatus;
        }else{
            return driverStatus;
        }
    }

}
