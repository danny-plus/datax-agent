package ni.danny.dataxagent.common.dto.datax.reader.hbase;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

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
