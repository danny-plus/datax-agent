package ni.danny.dataxagent.executor.dto.event;

import com.lmax.disruptor.EventFactory;

/**
 * @author bingobing
 */
public class ExecutorEventFactory implements EventFactory<ExecutorEvent> {
    @Override
    public ExecutorEvent newInstance() {
        return new ExecutorEvent();
    }
}
