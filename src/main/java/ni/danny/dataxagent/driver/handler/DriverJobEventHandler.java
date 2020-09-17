package ni.danny.dataxagent.driver.handler;

import com.lmax.disruptor.EventHandler;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.driver.dto.event.DriverJobEvent;
import ni.danny.dataxagent.driver.producer.DriverJobEventProducerWithTranslator;
import ni.danny.dataxagent.driver.service.DataxDriverJobService;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class DriverJobEventHandler implements EventHandler<DriverJobEvent> {

    @Autowired
    private DataxDriverJobService dataxDriverJobService;

    @Autowired
    private DataxDriverService dataxDriverService;



    @Override
    public void onEvent(DriverJobEvent event, long l, boolean b) throws Exception {
        if(!dataxDriverService.checkDriverIsSelf()){
            event.clear();
            return;
        }
        if(event.getDto().getDelayTime()>System.currentTimeMillis()){
            dataxDriverJobService.dispatchEvent(event.getDto());
            event.clear();
            return;
        }

        while(!ZookeeperConstant.STATUS_RUNNING.equals(ZookeeperConstant.driverJobEventHandlerStatus)){
            Thread.sleep(5*1000);
        }
        log.info(event.getDto().toString());
        switch (event.getDto().getType().toString()){
            case "JOB_SCAN": dataxDriverJobService.scanJob(null,null);break;
            case "JOB_CREATED": dataxDriverJobService.jobCreatedEvent(event.getDto());break;
            case "JOB_REJECTED": dataxDriverJobService.jobRejectedEvent(event.getDto());break;
            case "JOB_FINISHED": dataxDriverJobService.jobFinishedEvent(event.getDto());break;
            case "TASK_CREATED": dataxDriverJobService.taskCreatedEvent(event.getDto());break;
            case "TASK_REJECTED": dataxDriverJobService.taskRejectedEvent(event.getDto());break;
            case "TASK_FINISHED": dataxDriverJobService.taskFinishedEvent(event.getDto());break;
            case "TASK_THREAD_FINISHED": dataxDriverJobService.taskThreadFinishedEvent(event.getDto());break;
            case "TASK_THREAD_REJECTED": dataxDriverJobService.taskThreadRejectedEvent(event.getDto());break;
        }
        event.clear();
    }

}
