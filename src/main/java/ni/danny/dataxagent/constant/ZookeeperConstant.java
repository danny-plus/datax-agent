package ni.danny.dataxagent.constant;

public class ZookeeperConstant {
    public final static String ROOT_PATH = "/datax-agent";
    public final static String DRIVER_PATH = ROOT_PATH+"/driver";
    public final static String EXECUTOR_ROOT_PATH = ROOT_PATH+"/executor";

    /** 任务目录下所有节点均为永久节点 */
    public final static String JOB_EXECUTOR_ROOT_PATH = ROOT_PATH+"/job/executor";
}
