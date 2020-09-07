package ni.danny.dataxagent.service;

import ni.danny.dataxagent.dto.DataxDTO;

public interface DataxJobSpiltContextService {

    DataxJobSpiltStrategy getDataxJobSpiltStrategy(String type);
    void splitDataxJob(String type,String jobId, DataxDTO dataxDTO);
}
