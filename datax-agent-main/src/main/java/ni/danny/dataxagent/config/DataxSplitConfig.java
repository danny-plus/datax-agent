package ni.danny.dataxagent.config;

import ni.danny.dataxsplit.base.DataxSplitService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author bingobing
 */
@Configuration
@ComponentScan(value="ni.danny.dataxsplit.*",useDefaultFilters = false
        ,includeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = DataxSplitService.class) } )
public class DataxSplitConfig {


}
