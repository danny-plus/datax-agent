package ni.danny.dataxagent.monitor.controller;

import groovy.util.logging.Slf4j;
import ni.danny.dataxagent.monitor.dto.DataxJobSummaryDTO;
import ni.danny.dataxagent.monitor.dto.resp.RespDTO;
import ni.danny.dataxagent.monitor.dto.resp.SuccessRespDTO;
import ni.danny.dataxagent.monitor.service.JobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
public class JobController {

    @Autowired
    private JobService jobService;

    @GetMapping("/jobs")
    public RespDTO<List<DataxJobSummaryDTO>> getAllJob() throws Exception{
        return new SuccessRespDTO().data(jobService.getAllJob());
    }

    @GetMapping("/jobLogs")
    public RespDTO<List<String>> getJobLogs(@RequestParam String jobId) throws Exception{
        return new SuccessRespDTO<List<String>>().data(jobService.getJobLogs(jobId));
    }

}
