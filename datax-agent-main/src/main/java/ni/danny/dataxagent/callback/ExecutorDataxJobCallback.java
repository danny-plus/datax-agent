package ni.danny.dataxagent.callback;

public interface ExecutorDataxJobCallback {
    void finishTask();
    void throwException(Throwable ex);
}
