package ni.danny.dataxagent.controller;

import com.alibaba.datax.common.exception.DataXException;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.dto.ResponseDTO;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

@Slf4j
@ControllerAdvice
public class DataxExceptionHandler {

    @ExceptionHandler(value= DataXException.class)
    @ResponseBody
    public ResponseDTO dataxExceptionHandler(DataXException dataXException){
        log.info("catch DATAXException==>[{}]",dataXException);
        return new ResponseDTO<String>(dataXException.getErrorCode().getCode()
                ,dataXException.getErrorCode().getDescription()
                ,dataXException.getMessage());

    }
}
