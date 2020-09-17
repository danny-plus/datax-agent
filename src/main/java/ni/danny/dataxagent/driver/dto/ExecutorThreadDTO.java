package ni.danny.dataxagent.driver.dto;

import lombok.Data;

import java.util.Comparator;

@Data
public class ExecutorThreadDTO implements Comparator<ExecutorThreadDTO> {

    private String executor;
    private int thread;
    public ExecutorThreadDTO(String executor,int thread){
        this.executor = executor;
        this.thread = thread;
    }

    private int priority=10;
    @Override
    public int compare(ExecutorThreadDTO o1, ExecutorThreadDTO o2) {
        return o1.priority-o2.priority;
    }
}
