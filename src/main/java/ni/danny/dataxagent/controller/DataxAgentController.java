package ni.danny.dataxagent.controller;

import com.alibaba.datax.core.Engine;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.dto.ResponseDTO;
import ni.danny.dataxagent.enums.RespDTOEnum;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class DataxAgentController {

    @Value("${datax.home}")
    private String dataxHome;

    @GetMapping("/test")
    @ResponseBody
    public ResponseDTO<Object> test(@RequestParam String jobJsonPath) throws Throwable {
        System.setProperty("datax.home",dataxHome);
        String[] dataxArgs = {"-job",jobJsonPath,"-mode","standalone","-jobid","-1"};

        Engine.entry(dataxArgs);
        return RespDTOEnum.SUCCESS.getResponseDTO();
    }


}
