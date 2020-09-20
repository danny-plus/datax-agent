package ni.danny.dataxagent.service;

import ni.danny.dataxagent.dto.DataxDTO;

import java.util.List;

/**
 *
 */
public interface DataxJobSpiltStrategy {
    List<DataxDTO> spiltDataxJob(String jobId, DataxDTO dataxDTO);
}
