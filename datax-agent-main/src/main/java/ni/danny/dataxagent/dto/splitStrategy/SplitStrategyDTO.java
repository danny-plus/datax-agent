package ni.danny.dataxagent.dto.splitStrategy;


import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ni.danny.dataxsplit.base.enums.DataxSplitTypeEnum;

/**
 * @author bingobing
 */
@ToString
@Data
public class SplitStrategyDTO {
    private DataxSplitTypeEnum type;
    private String name;
    private String url;
    private String path;
    private Object strategy;


}
