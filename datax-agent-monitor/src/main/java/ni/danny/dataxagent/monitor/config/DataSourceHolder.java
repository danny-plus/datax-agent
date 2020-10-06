package ni.danny.dataxagent.monitor.config;

import ni.danny.dataxagent.monitor.util.DatabaseChooseUtils;

public class DataSourceHolder {
    private static final ThreadLocal<String> DATASOURCE = new ThreadLocal<>();

    public static void setDatasource(String datasource){
        DATASOURCE.set(datasource);
    }

    public static String getDatasource(){
           return DatabaseChooseUtils.getDBType().name();
    }
}
