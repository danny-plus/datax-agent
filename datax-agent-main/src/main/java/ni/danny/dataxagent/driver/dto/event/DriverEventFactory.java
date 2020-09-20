package ni.danny.dataxagent.driver.dto.event;

import com.lmax.disruptor.EventFactory;

public class DriverEventFactory implements EventFactory<DriverEvent> {

    @Override
    public DriverEvent newInstance(){
        return new DriverEvent();
    }
}
