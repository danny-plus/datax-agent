package ni.danny.dataxagent.driver.handler;

import com.lmax.disruptor.EventHandler;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.driver.dto.event.DriverEvent;
import ni.danny.dataxagent.driver.dto.event.DriverEventDTO;
import ni.danny.dataxagent.driver.dto.event.DriverJobEvent;
import ni.danny.dataxagent.driver.service.DataxDriverJobService;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

@Slf4j
public class DriverEventHandler implements EventHandler<DriverEvent> {

    @Autowired
    @Lazy
    private DataxDriverService dataxDriverService;

    @Override
    public void onEvent(DriverEvent event, long l, boolean b) throws Exception {
        log.debug("check is self => {} ",event.getDto().toString());
        if(!dataxDriverService.checkDriverIsSelf()){
            event.clear();
            return;
        }
        log.debug("check is time => delay Time = [{}],now time = [{}]",event.getDto().getDelayTime(),System.currentTimeMillis());
        DriverEventDTO dto = event.getDto();
        if(dto.getDelayTime()>System.currentTimeMillis()){
            event.clear();
            dataxDriverService.dispatchEvent(dto);
            return;
        }
        log.debug("time is now ==>{}",event.getDto().toString());
        if(dto.getRetryNum()>10){
            event.clear();
            return;
        }
        log.debug("can dispatch ==>{}",event.getDto().toString());
        switch (dto.getType().toString()){
            case "TASK_DISPATCH": dataxDriverService.dispatchTask(dto);break;
        }
        event.clear();
    }

}
