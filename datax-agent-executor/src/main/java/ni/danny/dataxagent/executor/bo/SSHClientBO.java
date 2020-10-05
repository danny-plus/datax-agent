package ni.danny.dataxagent.executor.bo;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author bingobing
 */
@Slf4j
@Builder
public class SSHClientBO {
    private String host;
    private Integer port;
    private String username;
    private String password;
    private String charset;

    private Connection connection;
    private boolean login(){
        connection = new Connection(this.host,this.port);
        try{
            connection.connect();
            return connection.authenticateWithPassword(this.username,this.password);
        }catch (IOException ioException){
            log.error("{}",ioException.getMessage());
            return false;
        }
    }

    private StringBuilder processStdout(InputStream in){
        byte[] buf = new byte[1024];
        StringBuilder builder = new StringBuilder();
        try{
            int length;
            while((length = in.read(buf)) != -1){
                builder.append(new String(buf,0,length));
            }
        } catch (IOException ioException) {
            log.error("{}",ioException.getMessage());
        }
        return builder;
    }

    public StringBuilder exec(String shell) throws IOException{
        InputStream inputStream = null;
        StringBuilder result = new StringBuilder();
        try{
            if(this.login()){
                Session session = connection.openSession();
                session.execCommand(shell);
                inputStream = session.getStdout();
                result = this.processStdout(inputStream);
                connection.close();
            }

        }finally {
            if(null != inputStream){
                inputStream.close();
            }
        }
        return result;
    }

}
