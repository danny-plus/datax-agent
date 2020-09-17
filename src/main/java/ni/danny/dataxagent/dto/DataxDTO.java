package ni.danny.dataxagent.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@Data
@ToString
public class DataxDTO {
    private Job job;
    private SplitStrategy splitStrategy;
    private String jobId;
    private int taskId;

    @Data
    @EqualsAndHashCode
    public class SplitStrategy{
        private String type;
        private Object strategy;
    }

    @Data
    @EqualsAndHashCode
    public class Job{
        private Content[] content;
        private Setting setting;

        @Data
        @EqualsAndHashCode
        public class Content{
            private Object reader;
            private Object writer;
        }

        @Data
        @EqualsAndHashCode
        public class Setting{
            private Speed speed;

            @Data
            @EqualsAndHashCode
            public class Speed{
                private String channel;
            }
        }
    }
}
