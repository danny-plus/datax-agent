package ni.danny.dataxagent.enums;

import lombok.Getter;
import ni.danny.dataxagent.common.dto.ResponseDTO;


public enum RespDTOEnum {
    /** success  */
    SUCCESS(new ResponseDTO("0000","SUCCESS")),
    /** 400  */
    BAD_REQUEST(new ResponseDTO("400","BAD-REQUEST"));

    @Getter
    private ResponseDTO responseDTO;

    RespDTOEnum(ResponseDTO responseDTO){
        this.responseDTO=responseDTO;
    }
}
