package ni.danny.dataxagent.service;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

public interface StartService extends ApplicationRunner {
    @Override
    void run(ApplicationArguments applicationArguments);
    void registerDriver();
    void registerExecutor();
}
