package br.com.selat.esperpoc.subscriber;

import br.com.selat.esperpoc.contract.TemperatureEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CriticalEventSubscriber implements StatementSubscriber{

    private static final String CRITICAL_EVENT_THRESHOLD = "100";
    private static final String CRITICAL_EVENT_MULTIPLIER = "1.5";
    private final Logger logger = LoggerFactory.getLogger(CriticalEventSubscriber.class);

    @Override
    public String getStatement() {
        return "select * from TemperatureEvent " +
                "match_recognize (" +
                "   measures A as temp1, B as temp2, C as temp3, D as temp4 " +
                "   pattern (A B C D)" +
                "   define " +
                "       A as A.temperature > " + CRITICAL_EVENT_THRESHOLD + ", " +
                "       B as (A.temperature < B.temperature), " +
                "       C as (B.temperature < C.temperature), " +
                "       D as (C.temperature < D.temperature) and D.temperature > (A.temperature * " + CRITICAL_EVENT_MULTIPLIER + ") )";
    }

    public void update(Map<String, TemperatureEvent> eventMap){
        TemperatureEvent temp1 = eventMap.get("temp1");
        TemperatureEvent temp2 = eventMap.get("temp2");
        TemperatureEvent temp3 = eventMap.get("temp3");
        TemperatureEvent temp4 = eventMap.get("temp4");

        StringBuilder sb = new StringBuilder();
        sb.append("*************************************\n");
        sb.append("* [ALERT]: CRITICAL EVENT DETECTED! *\n");
        sb.append(" " + temp1 + " > " + temp2 + " > " + temp3 + " > " + temp4 + "\n");
        sb.append("*************************************");
        logger.debug("{}", sb);
    }
}
