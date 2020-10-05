package ni.danny.dataxagent.executor;
import ni.danny.dataxagent.common.dto.*;
public interface ExecutorStrategyContent {
    void execute(DataxDTO dataxDTO,ExecutorCallback callback) throws Throwable;
}
