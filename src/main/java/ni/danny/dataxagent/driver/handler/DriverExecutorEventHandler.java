package ni.danny.dataxagent.driver.handler;

import com.lmax.disruptor.EventHandler;
import ni.danny.dataxagent.driver.dto.event.DriverExecutorEvent;

public class DriverExecutorEventHandler implements EventHandler<DriverExecutorEvent> {
    @Override
    public void onEvent(DriverExecutorEvent event, long l, boolean b) throws Exception {

        event.clear();
    }
}
