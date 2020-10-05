package ni.danny.dataxagent.monitor.config;

import ni.danny.dataxagent.monitor.enums.DataBaseType;
import ni.danny.dataxagent.monitor.util.InstallUtils;

public class DataSourceHolder {
    private static final ThreadLocal<String> DATASOURCE = new ThreadLocal<>();

    public static void setDatasource(String datasource){
        DATASOURCE.set(datasource);
    }

    public static String getDatasource(){
        if(InstallUtils.isInstall()){
            return DataBaseType.MYSQL.name();
        }else{
            return DataBaseType.H2.name();
        }
    }
}
