package ni.danny.dataxagent.executor.strategy;

import ni.danny.dataxagent.executor.ExecutorCallback;
import ni.danny.dataxagent.executor.ExecutorStrategy;
import org.springframework.stereotype.Component;

@Component("jarExecutor")
public class ExecutorByJarImpl implements ExecutorStrategy {
    @Override
    public String name() {
        return "jar";
    }
    @Override
    public void execute(DataxDTO dataxDTO, ExecutorCallback callback) {

    }
}
