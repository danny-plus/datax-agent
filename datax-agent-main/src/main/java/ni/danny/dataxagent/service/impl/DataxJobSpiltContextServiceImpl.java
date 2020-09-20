package ni.danny.dataxagent.service.impl;

import com.alibaba.datax.common.util.Configuration;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.service.DataxJobSpiltContextService;
import ni.danny.dataxagent.service.DataxJobSpiltStrategy;
import ni.danny.dataxsplit.base.DataxSplitCallback;
import ni.danny.dataxsplit.base.DataxSplitService;
import ni.danny.dataxsplit.base.enums.DataxSplitTypeEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Slf4j
@Service
public class DataxJobSpiltContextServiceImpl implements DataxJobSpiltContextService {

    @Override
    public List<DataxDTO> splitDataxJob(DataxDTO dataxDTO) {
        DataxSplitTypeEnum type = dataxDTO.getSplitStrategy().getType();
        String name = dataxDTO.getSplitStrategy().getName();
        log.info("type=[{}], name=[{}]",type,name);
        switch(type.toString()){
            case "LOCAL":return splitDataxJobByLocal(name,dataxDTO);
            case "REMOTE":return splitDataxJobByRemote(name,dataxDTO);
            default:return splitDataxJobByDepend(name,dataxDTO);
        }
    }

    @Autowired
    private List<DataxSplitService> dataxSplitServices;


    private DataxSplitService getDependDataxSplitService(String name){
        for(DataxSplitService service: dataxSplitServices){
            if(service.name().equals(name)){
                return service;
            }
        }
        return null;
    }

    @Autowired
    private Gson gson;

    private List<DataxDTO> splitDataxJobByDepend(String name, DataxDTO dataxDTO){
        List<DataxDTO> dataxDTOList = new ArrayList<>();
        DataxSplitService splitService = this.getDependDataxSplitService(name);

        splitService.split(gson.toJson(dataxDTO), configurationList -> {
           for(Configuration configuration:configurationList){
               DataxDTO newDataxDTO =  gson.fromJson(configuration.toJSON(),DataxDTO.class);
               dataxDTOList.add(newDataxDTO);
           }
        });
        return dataxDTOList;
    }

    //TODO 启动时加载指定文件目录下的拆解插件
    private List<DataxDTO> splitDataxJobByLocal(String name, DataxDTO dataxDTO){
        return null;
    }

    @Autowired
    private RestTemplate restTemplate;

    private List<DataxDTO> splitDataxJobByRemote(String name, DataxDTO dataxDTO){
        String url = dataxDTO.getSplitStrategy().getUrl();
        List<DataxDTO> result = restTemplate.postForObject(url,dataxDTO,ArrayList.class);
        return result;
    }
}
