package ni.danny.dataxagent.executor.producer;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import ni.danny.dataxagent.executor.dto.event.ExecutorEvent;
import ni.danny.dataxagent.executor.dto.event.ExecutorEventDTO;

/**
 * @author bingobing
 */
public class ExecutorEventProducerWithTranslator {

    private final RingBuffer<ExecutorEvent> ringBuffer;

    public ExecutorEventProducerWithTranslator(RingBuffer<ExecutorEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    private final EventTranslatorOneArg<ExecutorEvent, ExecutorEventDTO> TRANSLATOR_ONE_ARG =
            (driverEvent, l, dto) -> driverEvent.setDto(dto);

    public void onData(ExecutorEventDTO dto){
        ringBuffer.publishEvent(TRANSLATOR_ONE_ARG,dto);
    }
}
