package ni.danny.dataxagent.common.dto.resp;

import lombok.Getter;
import lombok.Setter;
import ni.danny.dataxagent.common.dto.ResponseDTO;

public class AnsycExcuteRespDTO extends ResponseDTO {

    @Getter
    @Setter
    private String taskId;

    @Getter
    @Setter
    private String jobId;

    @Getter
    @Setter
    private String traceId;

    public AnsycExcuteRespDTO(String code, String msg) {
        super(code, msg);
    }

    public AnsycExcuteRespDTO(String code,String msg,String taskId){
        super(code,msg);
        this.taskId = taskId;
    }

    public AnsycExcuteRespDTO(ResponseDTO enumsDto,String taskId,String jobId,String traceId){
        super(enumsDto);
        this.taskId = taskId;
        this.jobId = jobId;
        this.traceId = traceId;
    }
}
