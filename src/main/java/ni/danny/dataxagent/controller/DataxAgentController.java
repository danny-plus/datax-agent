package ni.danny.dataxagent.controller;

import com.alipay.common.tracer.core.context.trace.SofaTraceContext;
import com.alipay.common.tracer.core.holder.SofaTraceContextHolder;
import com.alipay.common.tracer.core.span.SofaTracerSpan;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.callback.ExecutorDataxJobCallback;
import ni.danny.dataxagent.dto.ResponseDTO;
import ni.danny.dataxagent.dto.resp.AnsycExcuteRespDTO;
import ni.danny.dataxagent.enums.RespDTOEnum;
import ni.danny.dataxagent.service.DataxAgentService;
import org.joda.time.DateTime;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;


@Slf4j
@RestController
public class DataxAgentController {

    @Autowired
    private DataxAgentService dataxAgentService;

    @GetMapping("/excuteJob")
    @ResponseBody
    public ResponseDTO excuteJob(@RequestParam String jobId,@RequestParam int taskId,@RequestParam String jobJsonPath) throws Throwable {
        dataxAgentService.asyncExecuteDataxJob(jobId, new Random().nextInt(999), jobJsonPath, new ExecutorDataxJobCallback() {
            @Override
            public void finishTask() {
                log.info("finish task");
            }
        });
        SofaTraceContext sofaTraceContext = SofaTraceContextHolder.getSofaTraceContext();
        SofaTracerSpan sofaTracerSpan = sofaTraceContext.getCurrentSpan();


        return new AnsycExcuteRespDTO(RespDTOEnum.SUCCESS.getResponseDTO()
                ,taskId+"",jobId,sofaTracerSpan.getSofaTracerSpanContext().getTraceId());
    }


}
