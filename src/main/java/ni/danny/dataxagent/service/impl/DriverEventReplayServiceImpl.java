package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.dto.ZookeeperEventDTO;
import ni.danny.dataxagent.service.DataxDriverService;
import ni.danny.dataxagent.service.DriverEventReplayService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import static ni.danny.dataxagent.constant.ZookeeperConstant.*;
import static ni.danny.dataxagent.constant.ZookeeperConstant.driverEventList;

@Slf4j
@Service
public class DriverEventReplayServiceImpl implements DriverEventReplayService {

    @Autowired
    private AppInfoComp appInfoComp;

    @Autowired
    private DataxDriverService dataxDriverService;

    @Override
    @Async("driverReplayThreadExecutor")
    public void replay() {
        log.info("start the ExecutorEventReplay");
        while(true){
            try{
                ZookeeperEventDTO zookeeperEventDTO = ZookeeperConstant.driverEventList.take();
                if(appInfoComp.getHostnameAndPort().equals(ZookeeperConstant.driverName)){
                    if(STATUS_RUNNING.equals(driverStatus)){
                        log.info("driver init finish, start to replay the change, size=[{}]",driverEventList.size());

                        switch (zookeeperEventDTO.getMethod()){
                            case "manageJobExecutorChange": dataxDriverService.manageJobExecutorChange(zookeeperEventDTO.getType(),zookeeperEventDTO.getOldData(),zookeeperEventDTO.getData());break;
                            case "managerExecutor": dataxDriverService.managerExecutor(zookeeperEventDTO.getType(),zookeeperEventDTO.getOldData(),zookeeperEventDTO.getData());break;
                            default:break;
                        }
                    }else{
                        ZookeeperConstant.driverEventList.add(
                                new ZookeeperEventDTO(zookeeperEventDTO.getMethod()
                                        ,zookeeperEventDTO.getType(),zookeeperEventDTO.getOldData(),zookeeperEventDTO.getData(),2*1000));

                    }
                }
            }catch (Exception ignore){

            }
        }

    }
}
