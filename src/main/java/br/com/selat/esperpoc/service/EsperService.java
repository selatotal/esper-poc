package br.com.selat.esperpoc.service;

import br.com.selat.esperpoc.contract.TemperatureEvent;
import br.com.selat.esperpoc.subscriber.CriticalEventSubscriber;
import br.com.selat.esperpoc.subscriber.MonitorEventSubscriber;
import br.com.selat.esperpoc.subscriber.StatementSubscriber;
import br.com.selat.esperpoc.subscriber.WarningEventSubscriber;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsperService {

    private final Logger logger = LoggerFactory.getLogger(EsperService.class);
    private final EPRuntime epRuntime;
    private final EPCompiler epCompiler;
    private final CompilerArguments args;
    private final StatementSubscriber criticalEventSubscriber;
    private final StatementSubscriber monitorEventSubscriber;
    private final StatementSubscriber warningEventSubscriber;

    public EsperService() throws EPCompileException, EPDeployException {
        Configuration config = new Configuration();
        config.getCommon().addEventType(TemperatureEvent.class);
        config.getCompiler().getByteCode().setAllowSubscriber(true);
        epCompiler = EPCompilerProvider.getCompiler();
        args = new CompilerArguments(config);

        epRuntime = EPRuntimeProvider.getDefaultRuntime(config);
        criticalEventSubscriber = new CriticalEventSubscriber();
        monitorEventSubscriber = new MonitorEventSubscriber();
        warningEventSubscriber = new WarningEventSubscriber();
        createCriticalTemperatureCheckExpression();
        createWarningTemperatureCheckExpression();
        createTemperatureMonitorExpression();
    }


    private void createCriticalTemperatureCheckExpression() throws EPCompileException, EPDeployException {
        logger.debug("create Critical Temperature Check Expression");
        EPCompiled criticalEventStatementLocal = epCompiler.compile(criticalEventSubscriber.getStatement(), args);
        EPDeployment deployment = epRuntime.getDeploymentService().deploy(criticalEventStatementLocal);
        EPStatement criticalEventStatement = epRuntime.getDeploymentService().getStatement(deployment.getDeploymentId(), "criticalEventStatement");
        criticalEventStatement.setSubscriber(criticalEventSubscriber);
    }

    private void createWarningTemperatureCheckExpression() throws EPCompileException, EPDeployException {
        logger.debug("create Warning Temperature Check Expression");
        EPCompiled warningEventStatementLocal = epCompiler.compile(warningEventSubscriber.getStatement(), args);
        EPDeployment deployment = epRuntime.getDeploymentService().deploy(warningEventStatementLocal);
        EPStatement warningEventStatement = epRuntime.getDeploymentService().getStatement(deployment.getDeploymentId(), "warningEventStatement");
        warningEventStatement.setSubscriber(warningEventSubscriber);
    }

    private void createTemperatureMonitorExpression() throws EPCompileException, EPDeployException {
        logger.debug("create Temperature Monitor Expression");
        EPCompiled monitorEventStatementLocal = epCompiler.compile(monitorEventSubscriber.getStatement(), args);
        EPDeployment deployment = epRuntime.getDeploymentService().deploy(monitorEventStatementLocal);
        EPStatement monitorEventStatement = epRuntime.getDeploymentService().getStatement(deployment.getDeploymentId(), "monitorEventStatement");
        monitorEventStatement.setSubscriber(monitorEventSubscriber);
    }

    public void handle(TemperatureEvent event) {
        logger.debug(event.toString());
        epRuntime.getEventService().sendEventBean(event, "TemperatureEvent");
    }
}
