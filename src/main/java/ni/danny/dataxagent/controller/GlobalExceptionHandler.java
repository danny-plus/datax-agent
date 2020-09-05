package ni.danny.dataxagent.controller;

import ni.danny.dataxagent.dto.ResponseDTO;
import ni.danny.dataxagent.enums.RespDTOEnum;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(value = MissingServletRequestParameterException.class)
    @ResponseBody
    public ResponseDTO<String> missingServletRequestParameterException(MissingServletRequestParameterException exception){

        return new ResponseDTO(RespDTOEnum.BAD_REQUEST.getResponseDTO(),exception.getMessage());
    }
}
