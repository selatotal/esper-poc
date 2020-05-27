package br.com.selat.esperpoc.subscriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MonitorEventSubscriber implements StatementSubscriber{

    private final Logger logger = LoggerFactory.getLogger(MonitorEventSubscriber.class);

    @Override
    public String getStatement() {
        return "@name('monitorEventStatement') select avg(temperature) as avg_val from TemperatureEvent.win:time_batch(5 sec) ";
    }

    public void update(Map<String, Double> eventMap){
        Double avg = eventMap.get("avg_val");

        StringBuilder sb = new StringBuilder();
        sb.append("*************************************\n");
        sb.append(" [MONITOR]: Average Temp = " + avg + "\n");
        sb.append("*************************************");
        logger.debug(sb.toString());
    }
}
