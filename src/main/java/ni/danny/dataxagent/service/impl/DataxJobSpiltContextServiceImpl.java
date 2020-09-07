package ni.danny.dataxagent.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.service.DataxJobSpiltContextService;
import ni.danny.dataxagent.service.DataxJobSpiltStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
public class DataxJobSpiltContextServiceImpl implements DataxJobSpiltContextService {

    @Autowired
    private Map<String,DataxJobSpiltStrategy> dataxJobSpiltStrategyMap;


    @Override
    public DataxJobSpiltStrategy getDataxJobSpiltStrategy(String type) {
        return dataxJobSpiltStrategyMap.get(type);
    }

    @Override
    public void splitDataxJob(String type,String jobId, DataxDTO dataxDTO) {
        dataxJobSpiltStrategyMap.get(type).spiltDataxJob(jobId, dataxDTO);
    }
}
