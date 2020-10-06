package ni.danny.dataxagent.monitor.dto.resp;

import lombok.Data;

public class SuccessRespDTO<T>  extends RespDTO{

    public SuccessRespDTO(){
        super("0000","SUCCESS",null);
    }

    public RespDTO data(T t){
        this.data=t;
        return this;
    }

}
