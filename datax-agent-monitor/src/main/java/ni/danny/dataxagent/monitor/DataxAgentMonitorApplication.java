package ni.danny.dataxagent.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * @author danny_ni
 */
@SpringBootApplication(exclude={DataSourceAutoConfiguration.class})
public class DataxAgentMonitorApplication {
    public static void main(String[] args) {
        SpringApplication.run(DataxAgentMonitorApplication.class, args);
    }
    @Bean
    public ObjectMapper objectMapper(){
        return new ObjectMapper();
    }
}
