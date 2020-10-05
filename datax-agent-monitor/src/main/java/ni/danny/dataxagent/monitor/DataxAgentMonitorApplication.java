package ni.danny.dataxagent.monitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 * @author danny_ni
 */
@SpringBootApplication(exclude={DataSourceAutoConfiguration.class})
public class DataxAgentMonitorApplication {
    public static void main(String[] args) {
        SpringApplication.run(DataxAgentMonitorApplication.class, args);
    }
}
