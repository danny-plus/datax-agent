package ni.danny.dataxagent.enums;

import lombok.Getter;
import ni.danny.dataxagent.dto.ResponseDTO;


public enum RespDTOEnum {
    SUCCESS(new ResponseDTO<Object>("0000","SUCCESS",null));


    @Getter
    private ResponseDTO<Object> responseDTO;

    RespDTOEnum(ResponseDTO<Object> responseDTO){
        this.responseDTO=responseDTO;
    }
}
