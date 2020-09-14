package ni.danny.dataxagent.driver.handler;

import com.lmax.disruptor.EventHandler;
import ni.danny.dataxagent.driver.dto.event.DriverJobEvent;

public class DriverJobEventHandler implements EventHandler<DriverJobEvent> {

    @Override
    public void onEvent(DriverJobEvent event, long l, boolean b) throws Exception {

        event.clear();
    }


}
