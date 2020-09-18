package ni.danny.dataxagent.dto.splitStrategy;

import lombok.Data;
import lombok.ToString;

@Data
public class OracleReaderSplitStrategyDTO  extends BaseReaderSpiltStrategyDTO{
    private String column;

    @Override
    public String toString() {
        return super.toString()+"OracleReaderSplitStrategyDTO{" +
                "column='" + column + '\'' +
                '}';
    }
}
