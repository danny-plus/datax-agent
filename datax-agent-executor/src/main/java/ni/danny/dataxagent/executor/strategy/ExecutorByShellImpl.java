package ni.danny.dataxagent.executor.strategy;

import ni.danny.dataxagent.executor.ExecutorCallback;
import ni.danny.dataxagent.executor.ExecutorStrategy;
import org.springframework.stereotype.Component;

@Component("shellExecutor")
public class ExecutorByShellImpl implements ExecutorStrategy {
    @Override
    public String name() {
        return "shell";
    }

    @Override
    public void execute(DataxDTO dataxDTO, ExecutorCallback callback) {

    }
}
