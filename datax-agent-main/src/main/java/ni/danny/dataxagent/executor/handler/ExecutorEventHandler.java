package ni.danny.dataxagent.executor.handler;

import com.lmax.disruptor.EventHandler;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.executor.dto.event.ExecutorEvent;
import ni.danny.dataxagent.executor.service.DataxExecutorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

/**
 * @author bingobing
 */
@Slf4j
public class ExecutorEventHandler implements EventHandler<ExecutorEvent> {
    @Autowired
    @Lazy
    private DataxExecutorService dataxExecutorService;

    @Override
    public void onEvent(ExecutorEvent event, long l, boolean b) throws Exception {

        if(event.getDto().getRetryNum()>5){
            event.clear();
            return ;
        }

        if(event.getDto().getDelayTime()>System.currentTimeMillis()){
            dataxExecutorService.dispatchEvent(event.getDto());
            event.clear();
            return;
        }
        dataxExecutorService.process(event.getDto().getNodeEventType(),event.getDto().getOldData(), event.getDto().getData());
        event.clear();
    }

}
