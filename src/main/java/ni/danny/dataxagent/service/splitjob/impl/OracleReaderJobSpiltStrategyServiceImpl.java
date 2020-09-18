package ni.danny.dataxagent.service.splitjob.impl;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.dto.DataxDTO;
import ni.danny.dataxagent.dto.datax.ContentDTO;
import ni.danny.dataxagent.dto.datax.JobDTO;
import ni.danny.dataxagent.dto.datax.reader.oracle.ConnectionDTO;
import ni.danny.dataxagent.dto.datax.reader.oracle.OracleReaderDTO;
import ni.danny.dataxagent.dto.datax.reader.oracle.ParameterDTO;
import ni.danny.dataxagent.dto.splitStrategy.OracleReaderSplitStrategyDTO;
import ni.danny.dataxagent.service.splitjob.OracleReaderJobSpiltStrategyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service("oracleReaderStrategy")
public class OracleReaderJobSpiltStrategyServiceImpl implements OracleReaderJobSpiltStrategyService {

    @Autowired
    private Gson gson;

    @Override
    public List<DataxDTO> spiltDataxJob(String jobId, DataxDTO dataxDTO) {
        OracleReaderSplitStrategyDTO splitStrategyDTO = (OracleReaderSplitStrategyDTO)dataxDTO.getSplitStrategy().getStrategy();
        String[][] splits = splitStrategyDTO.getSplits();
        List<DataxDTO> newDataxList = new ArrayList<>(splits.length);
        String column = splitStrategyDTO.getColumn();

        for(int l=0,w=splits.length;l<w;l++){
            String[] split=splits[l];
             ContentDTO[] contents = dataxDTO.getJob().getContent();
             ContentDTO[] newContents = new ContentDTO[contents.length];
             for(int k=0,y=contents.length;k<y;k++){
                ContentDTO content = contents[k];
                 OracleReaderDTO readerDTO =(OracleReaderDTO) content.getReader();
                 ConnectionDTO[] connections = readerDTO.getParameter().getConnection();
                 ConnectionDTO[] newConnections = new ConnectionDTO[connections.length];
                 for(int j=0,x=connections.length;j<x;j++){
                     ConnectionDTO connectionDTO=connections[j];
                     String[] querySqls = connectionDTO.getQuerySql();
                     String[] newQuerySqls = new String[querySqls.length];
                     for(int i=0,z=querySqls.length;i<z;i++){
                         String querySql = querySqls[i];
                         StringBuilder newSql = new StringBuilder();
                         newSql.append(querySql);
                         if(querySql.toUpperCase().contains("WHERE")){
                             newSql.append(" WHERE "+column+" >= '"+split[0]+"' and "+column+" < '"+split[1]+"' ");
                         }else {
                             newSql.append(" AND "+column+" >= '"+split[0]+"' AND "+column+" < '"+split[1]+"' ");
                         }
                         newQuerySqls[i] = newSql.toString();
                     }
                     newConnections[j] = new ConnectionDTO(newQuerySqls,connectionDTO.getJdbcUrl());
                     }
                 ParameterDTO newParameterDTO = new ParameterDTO(readerDTO.getParameter().getUsername()
                         ,readerDTO.getParameter().getPassword(),newConnections);
                 OracleReaderDTO newReaderDTO = new OracleReaderDTO(readerDTO.getName(),newParameterDTO);
                 newContents[k] = new ContentDTO(newReaderDTO,content.getWriter());
                 }
             JobDTO newjobDTO = new JobDTO(newContents,dataxDTO.getJob().getSetting());
             DataxDTO newDataxDTO = new DataxDTO(newjobDTO,null,dataxDTO.getJobId(),l+1);
            newDataxList.add(newDataxDTO);
             }

        return newDataxList;

    }
}
