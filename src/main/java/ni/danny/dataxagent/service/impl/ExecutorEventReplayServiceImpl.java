package ni.danny.dataxagent.service.impl;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant.*;
import ni.danny.dataxagent.dto.DataxExecutorTaskDTO;
import ni.danny.dataxagent.dto.ZookeeperEventDTO;
import ni.danny.dataxagent.service.DataxExecutorService;
import ni.danny.dataxagent.service.ExecutorEventReplayService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ExecutorEventReplayServiceImpl implements ExecutorEventReplayService {

    @Autowired
    private DataxExecutorService dataxExecutorService;

    @Autowired
    private Gson gson;

    @Override
    @Async("executorReplayThreadExecutor")
    public void replay() {
        log.info("start the ExecutorEventReplay");
        while(true){
            try{
                ZookeeperEventDTO zookeeperEventDTO = ZookeeperConstant.executorEventList.take();
                if(ZookeeperConstant.STATUS_RUNNING.equals(ZookeeperConstant.executorStatus)){
                    switch (zookeeperEventDTO.getMethod()){
                        case "process": dataxExecutorService.process(zookeeperEventDTO.getType(),zookeeperEventDTO.getOldData(),zookeeperEventDTO.getData());break;
                        case "rejectTask":
                            DataxExecutorTaskDTO rejectDto = gson.fromJson(new String(zookeeperEventDTO.getOldData().getData()),DataxExecutorTaskDTO.class);
                            dataxExecutorService.receiveTask(rejectDto);
                            break;
                        case "finishTask":
                            DataxExecutorTaskDTO finishDto = gson.fromJson(new String(zookeeperEventDTO.getOldData().getData()),DataxExecutorTaskDTO.class);
                            dataxExecutorService.finishTask(finishDto);
                            break;
                        default: break;
                    }
                }else{
                    ZookeeperConstant.executorEventList.add(
                            new ZookeeperEventDTO(zookeeperEventDTO.getMethod()
                                    ,zookeeperEventDTO.getType(),zookeeperEventDTO.getOldData(),zookeeperEventDTO.getData(),2*1000));
                }
            }catch (Exception ignore){}
        }
    }
}
