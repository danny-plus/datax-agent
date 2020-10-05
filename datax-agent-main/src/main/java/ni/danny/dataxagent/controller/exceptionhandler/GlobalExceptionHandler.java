package ni.danny.dataxagent.controller.exceptionhandler;

import ni.danny.dataxagent.common.dto.ResponseDTO;
import ni.danny.dataxagent.common.dto.resp.ExceptionRespDTO;
import ni.danny.dataxagent.enums.RespDTOEnum;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MissingServletRequestParameterException.class)
    @ResponseBody
    public ResponseDTO missingServletRequestParameterException(MissingServletRequestParameterException exception){

        return new ExceptionRespDTO(RespDTOEnum.BAD_REQUEST.getResponseDTO(),exception.getMessage());
    }
}
