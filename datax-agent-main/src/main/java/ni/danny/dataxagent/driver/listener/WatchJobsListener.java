package ni.danny.dataxagent.driver.listener;

import groovy.lang.Lazy;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import ni.danny.dataxagent.driver.service.DataxDriverExecutorService;
import ni.danny.dataxagent.driver.service.DataxDriverJobService;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Component
public class WatchJobsListener implements CuratorCacheListener {

    @Lazy
    @Resource
    private DataxDriverJobService dataxDriverJobService;

    @Lazy
    @Resource
    private DataxDriverExecutorService dataxDriverExecutorService;

    @Override
    public void event(Type type, ChildData oldData, ChildData data) {
        if(oldData!=null){
            dispatchEvent(oldData.getPath(),type,oldData,data);
        }else if(data!=null){
            dispatchEvent(data.getPath(),type,oldData,data);
        }else{
            log.error("unknow event type=[{}],oldData=[{}],data=[{}]",type,oldData,data);
        }
    }

    private void dispatchEvent(String path,Type type,ChildData oldData,ChildData data){
        if(path.startsWith(ZookeeperConstant.JOB_LIST_ROOT_PATH)){
            dataxDriverJobService.dispatchJobEvent(type,oldData,data);
        }else if(path.startsWith(ZookeeperConstant.JOB_EXECUTOR_ROOT_PATH)){
            dataxDriverExecutorService.dispatchJobExecutorEvent(type,oldData,data);
        }else{
            log.error("unknow event type=[{}],oldData=[{}],data=[{}]",type,oldData,data);
        }
    }
}
