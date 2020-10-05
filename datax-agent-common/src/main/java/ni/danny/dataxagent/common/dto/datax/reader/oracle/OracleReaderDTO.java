package ni.danny.dataxagent.common.dto.datax.reader.oracle;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
public class OracleReaderDTO {
    private String name;
    private ParameterDTO parameter;
}
