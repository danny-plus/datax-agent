package ni.danny.dataxagent.common.dto.datax.reader.hbase;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class ColumnDTO {
    private String name;
    private String type;
    private String format;
}
