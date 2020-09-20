package ni.danny.dataxagent.constant;

import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.dto.DataxLogDTO;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class DataxJobConstant {
    /**
     * 用于快速过滤在执行的重复任务
     */
    public static ConcurrentSkipListSet<DataxDTO> dataxDTOS = new ConcurrentSkipListSet<>();

    public static ConcurrentHashMap<String, DataxLogDTO> executorKafkaLogs = new ConcurrentHashMap<>();

    public final static String JOB_FINISH = "FINISH";
    public final static String JOB_REJECT = "REJECT";


    public final static String TASK_FINISH = "FINISH";
    public final static String TASK_REJECT = "REJECT";

    public final static String EXECUTOR_HEALTH_CHECK_URL = "/actuator/health";

    public final static String HEALTH_UP = "UP";

    public static AtomicInteger executorThreadNum = new AtomicInteger(0);
}
