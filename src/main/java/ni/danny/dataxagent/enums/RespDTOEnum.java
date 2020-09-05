package ni.danny.dataxagent.enums;

import lombok.Getter;
import ni.danny.dataxagent.dto.ResponseDTO;


public enum RespDTOEnum {
    /** success  */
    SUCCESS(new ResponseDTO<Object>("0000","SUCCESS",null)),
    /** 400  */
    BAD_REQUEST(new ResponseDTO<Object>("400","BAD-REQUEST",null));

    @Getter
    private ResponseDTO<Object> responseDTO;

    RespDTOEnum(ResponseDTO<Object> responseDTO){
        this.responseDTO=responseDTO;
    }
}
