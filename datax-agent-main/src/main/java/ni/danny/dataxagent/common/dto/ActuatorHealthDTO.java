package ni.danny.dataxagent.common.dto;

import lombok.Data;

@Data
public class ActuatorHealthDTO {
    private String status;
    private ComponentDTO components;

    @Data
    public class ComponentDTO{
        private DataxAgentExecutorPoolDTO dataxAgentExecutorPool;

        @Data
        public class DataxAgentExecutorPoolDTO{
            private String status;
            private DetailDTO  details;

            @Data
            public class DetailDTO{
                private Integer activeCount;
                private Integer corePoolSize;
                private Integer maxPoolSize;
                private Integer poolSize;
            }
        }

    }
}
