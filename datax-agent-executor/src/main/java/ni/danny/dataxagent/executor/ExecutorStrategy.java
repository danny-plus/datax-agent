package ni.danny.dataxagent.executor;

public interface ExecutorStrategy {
    String name();
    void execute(DataxDTO dataxDTO,ExecutorCallback callback);
}
