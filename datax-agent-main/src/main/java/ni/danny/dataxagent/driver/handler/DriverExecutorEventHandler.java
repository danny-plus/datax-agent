package ni.danny.dataxagent.driver.handler;

import com.lmax.disruptor.EventHandler;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.driver.dto.event.DriverExecutorEvent;
import ni.danny.dataxagent.driver.service.DataxDriverExecutorService;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

public class DriverExecutorEventHandler implements EventHandler<DriverExecutorEvent> {

    @Autowired
    @Lazy
    private DataxDriverExecutorService dataxDriverExecutorService;

    @Autowired
    @Lazy
    private DataxDriverService dataxDriverService;

    @Override
    public void onEvent(DriverExecutorEvent event, long l, boolean b) throws Exception {

        if(!dataxDriverService.checkDriverIsSelf()){
            event.clear();
            return;
        }
        if(event.getDto().getDelayTime()>System.currentTimeMillis()){
            event.clear();
            dataxDriverService.dispatchExecutorEvent(event.getDto());
            return;
        }

        while(!ZookeeperConstant.STATUS_RUNNING.equals(ZookeeperConstant.driverExecutorEventHandlerStatus)){
            Thread.sleep(5*1000);
        }

        switch (event.getDto().getType().toString()){
            case "EXECUTOR_SCAN": dataxDriverExecutorService.scanExecutor();break;
            case "EXECUTOR_UP":dataxDriverExecutorService.executorCreatedEvent(event.getDto()); break;
            case "EXECUTOR_DOWN": dataxDriverExecutorService.executorRemovedEvent(event.getDto());break;
            case "THREAD_CREATED": dataxDriverExecutorService.threadCreatedEvent(event.getDto());break;
            case "THREAD_WAITRECYCLE": dataxDriverExecutorService.threadUpdateWaitRecycleEvent(event.getDto());break;
            case "THREAD_READY,": dataxDriverExecutorService.threadUpdateReadyEvent(event.getDto());break;
            case "THREAD_REMOVED": dataxDriverExecutorService.threadRemovedEvent(event.getDto());break;
            case "THREAD_TASK_CREATED": dataxDriverExecutorService.threadTaskCreatedEvent(event.getDto());break;
            case "THREAD_TASK_SET_TRACEID": dataxDriverExecutorService.threadTaskUpdatedEvent(event.getDto());break;
            case "THREAD_TASK_REMOVED": dataxDriverExecutorService.threadTaskRemovedEvent(event.getDto()); break;
            default:break;
        }



    }
}
