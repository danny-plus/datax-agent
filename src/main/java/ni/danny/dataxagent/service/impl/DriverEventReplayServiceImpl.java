package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.dto.ZookeeperEventDTO;
import ni.danny.dataxagent.service.DriverEventReplayService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import static ni.danny.dataxagent.constant.ZookeeperConstant.*;
import static ni.danny.dataxagent.constant.ZookeeperConstant.driverEventList;

@Slf4j
@Service
public class DriverEventReplayServiceImpl implements DriverEventReplayService {

    @Override
    public void replayJobEvent() {

    }

    @Override
    public void replayExecutorEvent() {

    }
}
