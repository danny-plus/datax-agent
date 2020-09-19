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
        if(!dataxDriverService.checkDriverIsSelf()){
            event.clear();
            return;
        }
        DriverEventDTO dto = event.getDto();
        if(dto.getDelayTime()>System.currentTimeMillis()){
            dataxDriverService.dispatchEvent(dto);
            event.clear();
            return;
        }

        if(dto.getRetryNum()>10){
            event.clear();
            return;
        }else{
           dto = dto.delay();
        }

        switch (dto.getType().toString()){
            case "TASK_DISPATCH": dataxDriverService.dispatchTask(dto);break;
        }
        event.clear();
    }

}
