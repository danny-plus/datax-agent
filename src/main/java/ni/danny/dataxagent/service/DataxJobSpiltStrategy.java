package ni.danny.dataxagent.service;

import ni.danny.dataxagent.dto.DataxDTO;

/**
 *
 */
public interface DataxJobSpiltStrategy {
    void spiltDataxJob(String jobId, DataxDTO dataxDTO);
}
