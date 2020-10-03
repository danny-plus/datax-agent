package ni.danny.dataxagent.service;

import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxsplit.base.enums.DataxSplitTypeEnum;

import java.util.List;

public interface DataxJobSpiltContextService {

    List<DataxDTO> splitDataxJob(DataxDTO dataxDTO) throws Exception;
}
