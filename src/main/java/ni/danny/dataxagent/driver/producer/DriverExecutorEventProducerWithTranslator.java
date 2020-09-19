package ni.danny.dataxagent.driver.producer;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import ni.danny.dataxagent.driver.dto.event.DriverExecutorEvent;
import ni.danny.dataxagent.driver.dto.event.DriverExecutorEventDTO;


public class DriverExecutorEventProducerWithTranslator {

    private final RingBuffer<DriverExecutorEvent> ringBuffer;

    public DriverExecutorEventProducerWithTranslator(RingBuffer<DriverExecutorEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    private final EventTranslatorOneArg<DriverExecutorEvent, DriverExecutorEventDTO> TRANSLATOR_ONE_ARG =
            (driverExecutorEvent, l, dto) -> driverExecutorEvent.setDto(dto);

    public void onData(DriverExecutorEventDTO dto){
        ringBuffer.publishEvent(TRANSLATOR_ONE_ARG,dto);
    }

}
