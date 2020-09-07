package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.constant.ZookeeperConstant.*;
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
