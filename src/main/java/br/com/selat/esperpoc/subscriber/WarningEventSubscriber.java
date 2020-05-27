package br.com.selat.esperpoc.subscriber;

import br.com.selat.esperpoc.contract.TemperatureEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class WarningEventSubscriber implements StatementSubscriber{

    private static final String WARNING_EVENT_THRESHOLD = "400";
    private final Logger logger = LoggerFactory.getLogger(WarningEventSubscriber.class);

    @Override
    public String getStatement() {
        return "@name('warningEventStatement') select * from TemperatureEvent " +
                "match_recognize (" +
                "   measures A as temp1, B as temp2 " +
                "   pattern (A B)" +
                "   define " +
                "       A as A.temperature > " + WARNING_EVENT_THRESHOLD + ", " +
                "       B as B.temperature > " + WARNING_EVENT_THRESHOLD + " ) ";
    }

    public void update(Map<String, TemperatureEvent> eventMap){
        TemperatureEvent temp1 = eventMap.get("temp1");
        TemperatureEvent temp2 = eventMap.get("temp2");

        StringBuilder sb = new StringBuilder();
        sb.append("******************************************\n");
        sb.append("* [WARNING]: TEMPERATURE SPIKE DETECTED! *\n");
        sb.append(" " + temp1 + " > " + temp2 + "\n");
        sb.append("*************************************");
        logger.debug(sb.toString());
    }
}
