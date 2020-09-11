package ni.danny.dataxagent.driver.service.impl;

import ni.danny.dataxagent.config.AppInfoComp;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import ni.danny.dataxagent.dto.DataxDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static ni.danny.dataxagent.constant.ZookeeperConstant.HTTP_PROTOCOL_TAG;

@Service
public class DataxDriverServiceImpl implements DataxDriverService {

    @Autowired
    private AppInfoComp appInfoComp;

    private String driverInfo(){
        return HTTP_PROTOCOL_TAG+appInfoComp.getIpAndPort();
    }

    @Override
    public void regist() {


    }

    @Override
    public void listen() {

    }

    @Override
    public void init() {

    }

    @Override
    public void executorScanSuccessCallback() {

    }

    @Override
    public void executorScanFailCallback() {

    }

    @Override
    public void jobScanSuccessCallback() {

    }

    @Override
    public void jobScanFailCallback() {

    }

    @Override
    public DataxDTO createJob(DataxDTO dataxDTO) {
        return null;
    }
}
