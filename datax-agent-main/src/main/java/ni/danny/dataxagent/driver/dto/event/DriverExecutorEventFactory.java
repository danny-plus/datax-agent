package ni.danny.dataxagent.driver.dto.event;

import com.lmax.disruptor.EventFactory;

public class DriverExecutorEventFactory implements EventFactory<DriverExecutorEvent> {

    @Override
    public DriverExecutorEvent newInstance(){
        return new DriverExecutorEvent();
    }
}
