package ni.danny.dataxagent.enums.exception;

import lombok.Getter;

public enum  DataxAgentExceptionCodeEnum {

    JSON_EMPTY("001","json is empty");

    @Getter
    String code;

    @Getter
    String msg;

    DataxAgentExceptionCodeEnum(String code,String msg){
        this.code = code;
        this.msg = msg;
    }

}
