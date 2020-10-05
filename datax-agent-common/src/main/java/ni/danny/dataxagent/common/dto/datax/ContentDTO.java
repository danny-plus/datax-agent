package ni.danny.dataxagent.common.dto.datax;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class ContentDTO{
    private Object reader;
    private Object writer;
}
