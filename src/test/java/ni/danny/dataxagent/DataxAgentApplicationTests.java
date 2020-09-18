package ni.danny.dataxagent;


import com.alibaba.datax.common.util.Configuration;
import com.google.gson.Gson;
import ni.danny.dataxagent.dto.splitStrategy.BaseReaderSpiltStrategyDTO;
import ni.danny.dataxagent.dto.splitStrategy.OracleReaderSplitStrategyDTO;
import ni.danny.dataxagent.dto.splitStrategy.SplitStrategyDTO;

class DataxAgentApplicationTests {
    public static void main(String[] args){
        Gson gson = new Gson();
        String info="{\"type\":\"hbaseReaderStrategy\",\"strategy\":{\"splits\":[[\"1\",\"2\"],[\"3\",\"4\"]],\"column\":\"3333\"}}";
        SplitStrategyDTO dto =gson.fromJson(info,SplitStrategyDTO.class);
        System.out.println(dto.toString());

    }

}
