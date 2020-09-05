package ni.danny.dataxagent.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
public class ResponseDTO<T> {
    private String code;
    private String msg;

    public ResponseDTO(ResponseDTO enumsDto,T data){
        this.code = enumsDto.getCode();
        this.msg = enumsDto.getMsg();
        this.data = data;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private T data;
}
