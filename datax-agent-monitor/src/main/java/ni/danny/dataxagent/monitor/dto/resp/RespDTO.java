package ni.danny.dataxagent.monitor.dto.resp;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;


@AllArgsConstructor
public class RespDTO<T>{
    private String code;
    private String msg;
    protected T data;
}
