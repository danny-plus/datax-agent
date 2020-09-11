package ni.danny.dataxagent.driver.producer;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import ni.danny.dataxagent.driver.dto.event.DriverJobEvent;
import ni.danny.dataxagent.driver.dto.event.DriverJobEventDTO;

public class DriverJobEventProducerWithTranslator {

    private final RingBuffer<DriverJobEvent> ringBuffer;

    public DriverJobEventProducerWithTranslator(RingBuffer<DriverJobEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    private final EventTranslatorOneArg<DriverJobEvent, DriverJobEventDTO> TRANSLATOR_ONE_ARG =
            (driverEvent, l, dto) -> driverEvent.setDto(dto);

    public void onData(DriverJobEventDTO dto){
        ringBuffer.publishEvent(TRANSLATOR_ONE_ARG,dto);
    }
}
