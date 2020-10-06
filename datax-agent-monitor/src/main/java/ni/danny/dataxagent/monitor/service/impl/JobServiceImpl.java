package ni.danny.dataxagent.monitor.service.impl;

import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.monitor.dto.DataxJobSummaryDTO;
import ni.danny.dataxagent.monitor.service.JobService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author danny_ni
 */
@Slf4j
@Service
public class JobServiceImpl implements JobService {

    @Value("${datax.log.path}")
    private String dataxLogPath;

    @Override
    public List<DataxJobSummaryDTO> getAllJob() throws Exception{
        return null;
    }

    @Override
    public List<String> getJobLogs(String jobId) throws Exception {
        //读取日志文件，并生成MSGLIST
        String jobLogPath = dataxLogPath+"/"+jobId+".log";
        File file = new File(jobLogPath);
        if(!file.exists()){
            return null;
        }
        List<String> logList = new ArrayList<>();
        try {
            BufferedReader in = new BufferedReader(new FileReader(jobLogPath));
            String str;
            while ((str = in.readLine()) != null) {
                logList.add(str);
            }
        } catch (IOException e) {
            log.error("read log file error,=>[{}]",e.getMessage());
            return null;
        }

        return logList;
    }
}
