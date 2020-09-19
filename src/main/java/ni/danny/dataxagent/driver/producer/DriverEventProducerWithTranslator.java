package ni.danny.dataxagent.driver.producer;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import ni.danny.dataxagent.driver.dto.event.DriverEvent;
import ni.danny.dataxagent.driver.dto.event.DriverEventDTO;

public class DriverEventProducerWithTranslator {
    private final RingBuffer<DriverEvent> ringBuffer;

    public DriverEventProducerWithTranslator(RingBuffer<DriverEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    private final EventTranslatorOneArg<DriverEvent, DriverEventDTO> TRANSLATOR_ONE_ARG =
            (driverEvent, l, dto) -> driverEvent.setDto(dto);

    public void onData(DriverEventDTO dto){
        ringBuffer.publishEvent(TRANSLATOR_ONE_ARG,dto);
    }
}
