package ni.danny.dataxagent.executor;
import ni.danny.dataxagent.common.dto.*;
public interface ExecutorStrategyContent {
    void execute(String executor,String jobId,Integer taskId,String scriptPath,ExecutorCallback callback) throws Throwable;
}
