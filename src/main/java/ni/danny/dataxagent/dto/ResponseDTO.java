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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private T data;
}
