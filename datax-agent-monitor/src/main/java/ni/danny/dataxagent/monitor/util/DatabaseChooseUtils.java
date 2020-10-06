package ni.danny.dataxagent.monitor.util;

import ni.danny.dataxagent.monitor.enums.DataBaseType;

import java.io.File;

/**
 * @author danny_ni
 */
public class DatabaseChooseUtils {
    public static DataBaseType getDBType(){
        String installFile = DatabaseChooseUtils.class.getResource("/").getPath()+"/mysql.properties";
        File file = new File(installFile);
        if(file.exists()){
            return DataBaseType.MYSQL;
        }else{
            return DataBaseType.H2;
        }
    }
}
