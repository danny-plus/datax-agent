package ni.danny.dataxagent.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum ExecutorTaskStatusEnum {
    FINISH("FINISH"),
    REJECT("REJECT"),
    RUNNING("RUNNING"),
    INIT("INIT"),
    NOT_EXIST("NOT_EXIST"),
    NOT_SELF("NOT_SELF"),
    UNKOWN("UNKOWN");

    @Getter
    private String value;

    public static ExecutorTaskStatusEnum getTaskStatusByValue(String value){
        for(ExecutorTaskStatusEnum executorTaskStatusEnum : ExecutorTaskStatusEnum.values()){
            if(value.equals(executorTaskStatusEnum.getValue())){
                return executorTaskStatusEnum;
            }
        }
        return ExecutorTaskStatusEnum.UNKOWN;
    }
}
