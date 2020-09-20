package ni.danny.dataxagent.dto.datax;

import lombok.Data;
import lombok.EqualsAndHashCode;
@Data
@EqualsAndHashCode
public class SettingDTO {
    private Speed speed;

    @Data
    @EqualsAndHashCode
    public class Speed{
        private String channel;
    }

}
