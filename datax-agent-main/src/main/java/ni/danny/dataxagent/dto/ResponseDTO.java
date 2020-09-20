package ni.danny.dataxagent.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;


public class ResponseDTO {
    @Getter
    @Setter
    private String code;

    @Getter
    @Setter
    private String msg;

    public ResponseDTO(ResponseDTO enumsDto){
        this.code = enumsDto.getCode();
        this.msg = enumsDto.getMsg();
    }

    public ResponseDTO(String code,String msg){
        this.code = code;
        this.msg = msg;
    }


}
