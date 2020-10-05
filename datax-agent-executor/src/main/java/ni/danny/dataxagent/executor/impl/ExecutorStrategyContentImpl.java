package ni.danny.dataxagent.executor.impl;

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
    public void execute(DataxDTO dataxDTO, ExecutorCallback callback) throws Exception {
        ExecutorStrategy executor = null;
        for(ExecutorStrategy executorStrategy:executorStrategyList){
            if(dataxDTO.getExecutor()==null&&"jar".equals(executorStrategy.name())){
                executor = executorStrategy;
            }else if(dataxDTO.getExecutor().equals(executorStrategy.name())){
                executor = executorStrategy;
            }
        }
        if(executor!=null){
            executor.execute(dataxDTO,callback);
        }else{
            //TODO
            throw new Exception("");
        }
    }
}
