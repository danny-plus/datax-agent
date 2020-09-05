package ni.danny.dataxagent.service;

import ni.danny.dataxagent.dto.DataxDTO;

public interface DataxAgentService {
    String createDataxJobJsonFile(DataxDTO dataxDTO);

    /**
     * 同步方法：返回任务ID
     * @param jobJsonFilePath
     * @throws Throwable
     */
    void aysncExcuteDataxJob(String jobId,int taskId,String jobJsonFilePath) throws Throwable;
}
