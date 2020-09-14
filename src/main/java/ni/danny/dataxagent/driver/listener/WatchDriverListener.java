package ni.danny.dataxagent.driver.listener;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.driver.service.DataxDriverService;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Random;

@Slf4j
@Component
public class WatchDriverListener implements CuratorCacheListener {

    @Autowired
    private DataxDriverService dataxDriverService;

    @Override
    public void event(Type type, ChildData oldData, ChildData data) {
        log.info("driver changed ====> {} ",type);
        if(type== CuratorCacheListener.Type.NODE_DELETED){
            log.info("sleep random millseconds then try be driver ");
            try{
                Thread.sleep(new Random().nextInt(3*1000));
            }catch (InterruptedException ignore){

            }
            dataxDriverService.regist();
        }
    }
}
