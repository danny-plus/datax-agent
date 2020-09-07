package ni.danny.dataxagent.service.splitjob.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.service.splitjob.OracleReaderJobSpiltStrategyService;
import org.springframework.stereotype.Service;

@Slf4j
@Service("oracleReaderStrategy")
public class OracleReaderJobSpiltStrategyServiceImpl implements OracleReaderJobSpiltStrategyService {
    @Override
    public void spiltDataxJob(String jobId, DataxDTO dataxDTO) {

    }
}
