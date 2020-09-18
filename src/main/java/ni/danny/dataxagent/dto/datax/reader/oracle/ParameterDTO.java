package ni.danny.dataxagent.dto.datax.reader.oracle;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class ParameterDTO{
    private String username;
    private String password;
    private ConnectionDTO[] connection;
}
