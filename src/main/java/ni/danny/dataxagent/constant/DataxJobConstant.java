package ni.danny.dataxagent.constant;

import ni.danny.dataxagent.dto.DataxDTO;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class DataxJobConstant {
    /**
     * 用于快速过滤在执行的重复任务
     */
    public static ConcurrentSkipListSet<DataxDTO> dataxDTOS = new ConcurrentSkipListSet<>();

    public final static String JOB_FINISH = "FINISH";
}
