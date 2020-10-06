package ni.danny.dataxagent.monitor.service.impl;

import groovy.util.logging.Slf4j;
import ni.danny.dataxagent.monitor.dto.DataxJobSummaryDTO;
import ni.danny.dataxagent.monitor.service.JobService;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class JobServiceImpl implements JobService {
    @Override
    public List<DataxJobSummaryDTO> getAllJob() throws Exception{
        return null;
    }

    @Override
    public List<String> getJobLogs() throws Exception {
        return null;
    }
}
