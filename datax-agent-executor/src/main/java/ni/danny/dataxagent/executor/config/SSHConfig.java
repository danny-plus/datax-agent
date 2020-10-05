package ni.danny.dataxagent.executor.config;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.executor.bo.SSHClientBO;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Slf4j
@Configuration
public class SSHConfig {

    private String host;
    private int port;
    private String user;
    private String password;
    private String charset;

    @Bean
    public SSHClientBO sshClient(){
      return  SSHClientBO.builder().host(host).port(port)
                .username(user).password(password).charset(charset).build();
    }
}
