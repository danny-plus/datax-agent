package ni.danny.dataxagent.service;

import ni.danny.dataxagent.callback.ExecutorDataxJobCallback;
import ni.danny.dataxagent.dto.DataxDTO;

public interface DataxAgentService {
    String createDataxJobJsonFile(DataxDTO dataxDTO);

    String createDataxJobJsonFile(String taskName,String json);

    /**
     * 同步方法：返回任务ID
     * @param jobJsonFilePath
     * @throws Throwable
     */
    void asyncExecuteDataxJob(String jobId, int taskId, String jobJsonFilePath, ExecutorDataxJobCallback callback) throws Throwable;
}
