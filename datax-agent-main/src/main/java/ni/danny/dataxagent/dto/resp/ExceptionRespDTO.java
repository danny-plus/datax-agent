package ni.danny.dataxagent.dto.resp;

import lombok.*;
import ni.danny.dataxagent.dto.ResponseDTO;

public class ExceptionRespDTO extends ResponseDTO {

    @Getter
    @Setter
    private String exceptionMsg;

    public ExceptionRespDTO(String code, String msg) {
        super(code, msg);
    }

    public ExceptionRespDTO(String code, String msg,String exceptionMsg) {
        super(code, msg);
        this.exceptionMsg = exceptionMsg;
    }

    public ExceptionRespDTO(ResponseDTO enumsDto,String exceptionMsg){
        super(enumsDto);
        this.exceptionMsg = exceptionMsg;
    }
}
