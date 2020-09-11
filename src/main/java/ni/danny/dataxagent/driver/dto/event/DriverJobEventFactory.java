package ni.danny.dataxagent.driver.dto.event;

import com.lmax.disruptor.EventFactory;

public class DriverJobEventFactory implements EventFactory<DriverJobEvent> {
    @Override
    public DriverJobEvent newInstance() {
        return new DriverJobEvent();
    }
}
