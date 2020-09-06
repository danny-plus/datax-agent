package ni.danny.dataxagent.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
@Component
public class AppInfoComp {

    @Value("${server.port}")
    private String port ;

    public String getHostnameAndPort(){
        InetAddress localHost = null;
        try {
            localHost = Inet4Address.getLocalHost();
        } catch (UnknownHostException e) {
            log.error(e.getMessage(),e);
        }
        String hostname = localHost.getHostName();
        return hostname+":"+port;
    }

    public String getIpAndPort(){
        InetAddress localHost = null;
        try {
            localHost = Inet4Address.getLocalHost();
        } catch (UnknownHostException e) {
            log.error(e.getMessage(),e);
        }
        String ip = localHost.getHostAddress();
        return ip+":"+port;
    }

}
