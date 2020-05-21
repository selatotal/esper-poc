package br.com.selat.esperpoc.service;

import br.com.selat.esperpoc.contract.TemperatureEvent;
import br.com.selat.esperpoc.subscriber.CriticalEventSubscriber;
import br.com.selat.esperpoc.subscriber.MonitorEventSubscriber;
import br.com.selat.esperpoc.subscriber.StatementSubscriber;
import br.com.selat.esperpoc.subscriber.WarningEventSubscriber;
import com.espertech.esper.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsperService {

    private final Logger logger = LoggerFactory.getLogger(EsperService.class);
    private final EPServiceProvider epService;
    private final StatementSubscriber criticalEventSubscriber;
    private final StatementSubscriber monitorEventSubscriber;
    private final StatementSubscriber warningEventSubscriber;
    private EPStatement criticalEventStatement;
    private EPStatement warningEventStatement;
    private EPStatement monitorEventStatement;

    public EsperService() {
        Configuration config = new Configuration();
        config.addEventTypeAutoName("br.com.selat.esperpoc.contract");
        epService = EPServiceProviderManager.getDefaultProvider(config);
        criticalEventSubscriber = new CriticalEventSubscriber();
        monitorEventSubscriber = new MonitorEventSubscriber();
        warningEventSubscriber = new WarningEventSubscriber();
        createCriticalTemperatureCheckExpression();
        createWarningTemperatureCheckExpression();
        createTemperatureMonitorExpression();
    }


    private void createCriticalTemperatureCheckExpression() {
        logger.debug("create Critical Temperature Check Expression");
        criticalEventStatement = epService.getEPAdministrator().createEPL(criticalEventSubscriber.getStatement());
        criticalEventStatement.setSubscriber(criticalEventSubscriber);
    }

    private void createWarningTemperatureCheckExpression() {
        logger.debug("create Warning Temperature Check Expression");
        warningEventStatement = epService.getEPAdministrator().createEPL(warningEventSubscriber.getStatement());
        warningEventStatement.setSubscriber(warningEventSubscriber);
    }

    private void createTemperatureMonitorExpression() {
        logger.debug("create Temperature Monitor Expression");
        monitorEventStatement = epService.getEPAdministrator().createEPL(monitorEventSubscriber.getStatement());
        monitorEventStatement.setSubscriber(monitorEventSubscriber);
    }

    public void handle(TemperatureEvent event) {
        logger.debug(event.toString());
        epService.getEPRuntime().sendEvent(event);
    }
}
