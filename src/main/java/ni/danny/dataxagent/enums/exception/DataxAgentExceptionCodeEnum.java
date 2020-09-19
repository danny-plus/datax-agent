package ni.danny.dataxagent.enums.exception;

import lombok.Getter;

public enum  DataxAgentExceptionCodeEnum {

    JSON_EMPTY("001","json is empty"),
    REPEAT_JOB("100","the job is repeat or has same jobId"),
    JOBID_CONTAINS_DASH("102","the jobid contains dash('-')");

    @Getter
    String code;

    @Getter
    String msg;

    DataxAgentExceptionCodeEnum(String code,String msg){
        this.code = code;
        this.msg = msg;
    }

}
