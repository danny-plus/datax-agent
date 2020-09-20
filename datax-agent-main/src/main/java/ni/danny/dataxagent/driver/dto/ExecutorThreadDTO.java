package ni.danny.dataxagent.driver.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Comparator;
import java.util.Objects;

@Data
@ToString
public class ExecutorThreadDTO implements Comparable<ExecutorThreadDTO> {
    private String id;
    private String executor;
    private int thread;
    public ExecutorThreadDTO(String executor,int thread){
        this.executor = executor;
        this.thread = thread;
        this.id = this.executor+"-"+this.thread;
        this.priority = this.id.hashCode();
    }

    private int priority;

    public ExecutorThreadDTO priority(int priority){
        this.priority = this.id.hashCode()+priority;
        return this;
    }

    @Override
    public int compareTo(ExecutorThreadDTO o) {
        return priority-o.priority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExecutorThreadDTO that = (ExecutorThreadDTO) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
