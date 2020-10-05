package ni.danny.dataxagent.executor;

/**
 * @author bingobing
 */
public interface ExecutorCallback {
    void success();
    void fail();
    void throwException(Throwable throwable);
}
