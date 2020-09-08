package ni.danny.dataxagent.service;

import ni.danny.dataxagent.callback.ExecutorDataxJobCallback;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.exception.DataxAgentException;

import java.io.IOException;

public interface DataxAgentService {
    String createDataxJobJsonFile(DataxDTO dataxDTO) throws IOException, DataxAgentException;

    String createDataxJobJsonFile(String taskName,String json) throws IOException, DataxAgentException;

    /**
     * 同步方法：返回任务ID
     * @param jobJsonFilePath
     * @throws Throwable
     */
    void asyncExecuteDataxJob(String jobId, int taskId, String jobJsonFilePath, ExecutorDataxJobCallback callback) throws Throwable;
}
