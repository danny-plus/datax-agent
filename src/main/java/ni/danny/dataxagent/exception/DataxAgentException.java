package ni.danny.dataxagent.exception;

import ni.danny.dataxagent.enums.exception.DataxAgentExceptionCodeEnum;

public class DataxAgentException extends Exception {
    private String errType;
    private String errCode;
    private String errMsg;
    private String errDesc;

    public DataxAgentException(String errType,String errCode,String errMsg,String errDesc){
        this.errType = errType;
        this.errCode = errCode;
        this.errDesc = errDesc;
        this.errMsg = errMsg;
    }

    private static String getType(){
        return "";
    }

    public static DataxAgentException create(DataxAgentExceptionCodeEnum codeEnum,String errDesc){
        return new DataxAgentException(getType(),codeEnum.getCode(),codeEnum.getMsg(),errDesc);
    }
}
