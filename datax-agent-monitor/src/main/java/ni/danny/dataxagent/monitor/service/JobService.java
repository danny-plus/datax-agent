package ni.danny.dataxagent.monitor.service;

import ni.danny.dataxagent.monitor.dto.DataxJobSummaryDTO;

import java.util.List;

public interface JobService {
    List<DataxJobSummaryDTO> getAllJob() throws Exception;
    List<String> getJobLogs() throws Exception;
}
