package ni.danny.dataxagent.dto.datax.reader.hbase;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.util.HashMap;

@Data
@AllArgsConstructor
@ToString
public class ParameterDTO {
    private Object hbaseConfig;
    private String table;
    private String encoding;
    private String mode;
    private ColumnDTO[] column;
    private RangeDTO range;
}
