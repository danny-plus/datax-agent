package ni.danny.dataxagent.executor.impl;

import ni.danny.dataxagent.common.dto.DataxDTO;
import ni.danny.dataxagent.common.exception.DataxAgentExecutorException;
import ni.danny.dataxagent.executor.ExecutorCallback;
import ni.danny.dataxagent.executor.ExecutorStrategy;
import ni.danny.dataxagent.executor.ExecutorStrategyContent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class ExecutorStrategyContentImpl implements ExecutorStrategyContent {

    @Autowired
    private List<ExecutorStrategy> executorStrategyList;

    @Override
    public void execute(String executorName,String jobId,Integer taskId,String scriptPath, ExecutorCallback callback) throws Exception {
        ExecutorStrategy executor = null;
        for(ExecutorStrategy executorStrategy:executorStrategyList){
            if(executorName.equals(executorStrategy.name())){
                executor = executorStrategy;
            }
        }
        if(executor!=null){
            executor.execute( jobId,taskId, scriptPath,callback);
        }else{
            //TODO
            throw DataxAgentExecutorException.builder().build();
        }
    }
}
