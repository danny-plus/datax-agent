package ni.danny.dataxagent.service;

import ni.danny.dataxagent.dto.DataxDTO;

import java.util.List;

public interface DataxJobSpiltContextService {

    DataxJobSpiltStrategy getDataxJobSpiltStrategy(String type);
    List<DataxDTO> splitDataxJob(String type, String jobId, DataxDTO dataxDTO);
}
