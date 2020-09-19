package ni.danny.dataxagent.service.splitjob.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.service.splitjob.HBaseReaderJobSpiltStrategyService;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service("hbaseReaderStrategy")
public class HBaseReaderJobSpiltStrategyServiceImpl implements HBaseReaderJobSpiltStrategyService {
    @Override
    public List<DataxDTO> spiltDataxJob(String jobId, DataxDTO dataxDTO) {


        return null;

    }
}
