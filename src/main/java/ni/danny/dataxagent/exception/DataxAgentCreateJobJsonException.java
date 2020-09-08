package ni.danny.dataxagent.exception;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;


public class DataxAgentCreateJobJsonException extends DataxAgentException {

    private DataxAgentCreateJobJsonException(String errType, String errCode, String errMsg, String errDesc) {
        super(errType, errCode, errMsg, errDesc);
    }

    private static String getType(){
        return "E01";
    }


}
