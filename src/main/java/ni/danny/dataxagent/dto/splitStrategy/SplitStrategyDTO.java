package ni.danny.dataxagent.dto.splitStrategy;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@Getter
public class SplitStrategyDTO {
    @Setter
    private String type;
    @Setter
    private Object strategy;


}
