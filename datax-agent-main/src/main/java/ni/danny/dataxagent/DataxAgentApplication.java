package ni.danny.dataxagent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ni.danny.dataxagent.constant.ZookeeperConstant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class DataxAgentApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataxAgentApplication.class, args);
    }



    @Bean
    public Gson gson(){
        GsonBuilder builder = new GsonBuilder();
        return  builder.create();
    }

    @Bean
    public RestTemplate restTemplate(){
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(1000);
        requestFactory.setReadTimeout(1000);
        return new RestTemplate(requestFactory);
    }




}
