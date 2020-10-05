package ni.danny.dataxagent.service;

import ni.danny.dataxagent.common.dto.DataxDTO;

import java.util.List;

public interface DataxJobSpiltContextService {

    List<DataxDTO> splitDataxJob(DataxDTO dataxDTO) throws Exception;
}
