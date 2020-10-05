package ni.danny.dataxagent.monitor.util;

import java.io.File;

public class InstallUtils {
    public static boolean isInstall(){
        String installFile = InstallUtils.class.getResource("/").getPath()+"/install.back";
        File file = new File(installFile);
        if(file.exists()){
            return true;
        }else{
            return false;
        }
    }
}
