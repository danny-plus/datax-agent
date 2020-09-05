package ni.danny.dataxagent.dto;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class DataxDTO {
    private Job job;

    @Data
    public class Job{
        private Content[] content;
        private Setting setting;

        @Data
        public class Content{
            private Object reader;
            private Object writer;
        }

        @Data
        public class Setting{
            private Speed speed;

            @Data
            public class Speed{
                private String channel;
            }
        }
    }
}
