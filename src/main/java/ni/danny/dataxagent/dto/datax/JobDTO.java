package ni.danny.dataxagent.dto.datax;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class JobDTO{
    private ContentDTO[] content;
    private SettingDTO setting;
}
