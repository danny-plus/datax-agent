package ni.danny.dataxagent.dto.datax.reader;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import ni.danny.dataxagent.dto.datax.reader.hbase.ParameterDTO;

@Data
@AllArgsConstructor
@ToString
public class HBaseReaderDTO {
    private String name;
    private ParameterDTO parameter;
}
