package ni.danny.dataxagent.monitor.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class PropertiesUtils {
    public static Properties getProperties(String fileName)  {
        Properties properties = new Properties();
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader("./"+fileName));
            properties.load(bufferedReader);
        }catch (IOException ioException){

        }

        return properties;
    }
}
