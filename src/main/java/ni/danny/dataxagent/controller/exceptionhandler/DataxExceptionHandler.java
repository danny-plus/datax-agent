package ni.danny.dataxagent.controller.exceptionhandler;

import com.alibaba.datax.common.exception.DataXException;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.dto.ResponseDTO;
import ni.danny.dataxagent.dto.resp.ExceptionRespDTO;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

@Slf4j
@ControllerAdvice
public class DataxExceptionHandler {

    @ExceptionHandler(value= DataXException.class)
    @ResponseBody
    public ResponseDTO dataxExceptionHandler(DataXException dataxException){
        return new ExceptionRespDTO(dataxException.getErrorCode().getCode()
                ,dataxException.getErrorCode().getDescription(),dataxException.getMessage());

    }
}
