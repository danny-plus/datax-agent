package ni.danny.dataxagent.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum ExecutorTaskStatusEnum {
    INIT("INIT",new String[]{"START","REJECT"}),
    START("START",new String[]{"RUNNING","REJECT"}),
    RUNNING("RUNNING",new String[]{"FINISH","REJECT"}),
    FINISH("FINISH",null),
    REJECT("REJECT",new String[]{"START","INIT"}),
    NOT_EXIST("NOT_EXIST",null),
    NOT_SELF("NOT_SELF",null),
    REMOVED("REMOVED",null),
    UNKOWN("UNKOWN",null);

    @Getter
    private String value;

    @Getter
    private String[] canNext;


    public static ExecutorTaskStatusEnum getTaskStatusByValue(String value){
        for(ExecutorTaskStatusEnum executorTaskStatusEnum : ExecutorTaskStatusEnum.values()){
            if(value.equals(executorTaskStatusEnum.getValue())){
                return executorTaskStatusEnum;
            }
        }
        return ExecutorTaskStatusEnum.UNKOWN;
    }
}
