package ni.danny.dataxagent.monitor.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceBuilder;
import com.alibaba.druid.support.spring.stat.DruidStatInterceptor;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxagent.monitor.enums.DataBaseType;
import ni.danny.dataxagent.monitor.util.InstallUtils;
import ni.danny.dataxagent.monitor.util.PropertiesUtils;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.aop.support.JdkRegexpMethodPointcut;
import org.springframework.boot.autoconfigure.condition.ConditionalOnResource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Configuration
public class DataSourceConfig {
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.druid.h2")
    public DataSource h2DataSource(DruidProperty druidProperty){
        DruidDataSource dataSource = DruidDataSourceBuilder.create().build();
        return druidProperty.druidDataSource(dataSource);
    }
    /**
     * 配置数据库后使用该数据源
     * @param druidProperty     druid配置属性
     * @return                  DruidDataSource
     */
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.druid.mysql")
    @ConditionalOnResource(resources = "classpath:install.back")
    public DataSource mysqlDataSource(DruidProperty druidProperty){
        DruidDataSource dataSource = DruidDataSourceBuilder.create().build();
        Properties properties = PropertiesUtils.getProperties("mysql.properties");
        dataSource.setUrl(properties.getProperty("url"));
        dataSource.setUsername(properties.getProperty("username"));
        dataSource.setPassword(properties.getProperty("password"));
        return druidProperty.druidDataSource(dataSource);
    }

    @Bean(name = "dynamicDataSource")
    @Primary
    public DynamicDataSource dynamicDataSource(DataSource h2DataSource,DataSource mysqlDataSource){
        Map<Object,Object> targetDataSource = new HashMap<>(2);
        targetDataSource.put(DataBaseType.H2.name(),h2DataSource);
        targetDataSource.put(DataBaseType.MYSQL.name(),mysqlDataSource);
        if(InstallUtils.isInstall()){
            return new DynamicDataSource(mysqlDataSource,targetDataSource);
        }else{
            return new DynamicDataSource(h2DataSource,targetDataSource);
        }
    }

    @Bean
    public DruidStatInterceptor druidStatInterceptor(){
        return new DruidStatInterceptor();
    }

    @Bean
    @Scope("prototype")
    public JdkRegexpMethodPointcut jdkRegexpMethodPointcut(){
        JdkRegexpMethodPointcut pointcut = new JdkRegexpMethodPointcut();
        pointcut.setPatterns("com.ramostear.blogdemo.*");
        return pointcut;
    }

    @Bean
    public DefaultPointcutAdvisor defaultPointcutAdvisor(DruidStatInterceptor druidStatInterceptor, JdkRegexpMethodPointcut pointcut){
        DefaultPointcutAdvisor advisor = new DefaultPointcutAdvisor();
        advisor.setPointcut(pointcut);
        advisor.setAdvice(druidStatInterceptor);
        return advisor;
    }

}
