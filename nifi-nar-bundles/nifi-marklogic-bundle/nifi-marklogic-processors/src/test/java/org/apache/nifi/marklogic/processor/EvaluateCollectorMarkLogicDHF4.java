package org.apache.nifi.marklogic.processor;

import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.DocumentMetadataHandle.DocumentCollections;
import com.marklogic.junit5.spring.AbstractSpringMarkLogicTest;

import org.apache.nifi.marklogic.controller.DefaultMarkLogicDatabaseClientService;
import org.apache.nifi.marklogic.controller.MarkLogicDatabaseClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This just focuses on evaluating the collector script against MarkLogic, as that can't be done in
 * EvaluateCollectorMarkLogicTest.
 */
@ContextConfiguration(classes = {TestConfigDHF4.class})
public class EvaluateCollectorMarkLogicDHF4 extends AbstractSpringMarkLogicTest {

    @Autowired
    protected TestConfigDHF4 testConfig4;

    protected MarkLogicDatabaseClientService service;
    protected String databaseClientServiceIdentifier = "databaseClientService";

    protected void addDatabaseClientService(TestRunner runner) {
        service = new DefaultMarkLogicDatabaseClientService();
        try {
            runner.addControllerService(databaseClientServiceIdentifier, service);
        } catch (InitializationException e) {
            throw new RuntimeException(e);
        }
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.HOST, testConfig4.getHost());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PORT, testConfig4.getRestPort().toString());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.USERNAME, testConfig4.getUsername());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PASSWORD, testConfig4.getPassword());
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.DATABASE,testConfig4.getDatabase());

        runner.enableControllerService(service);
    }

    protected TestRunner getNewTestRunner(Class processor) {
        TestRunner runner = TestRunners.newTestRunner(processor);
        addDatabaseClientService(runner);       
        assertTrue(runner.isControllerServiceEnabled(service));
        runner.assertValid(service);
        runner.setProperty(AbstractMarkLogicProcessor.DATABASE_CLIENT_SERVICE, databaseClientServiceIdentifier);
        return runner;
    }
    
	@BeforeEach
	public void setup() {
		JSONDocumentManager mgr = getDatabaseClient().newJSONDocumentManager();
        DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
        DocumentCollections collections = metadataHandle.getCollections();
        collections.add("EVALUATE-COLLECTOR-TEST");
        metadataHandle.setCollections(collections);
        
		for (int i = 0; i < 10; i++) {
			mgr.write("/sample/dhf4/" + i + ".json", metadataHandle,new StringHandle(format("{\"test\":\"%d\"}", i)));
		}
	}
	
	@Test
	public void invokeCollector4() {
		TestRunner runner = getNewTestRunner(EvaluateCollectorMarkLogic.class);
		runner.setValidateExpressionUsage(false);
		runner.setProperty(EvaluateCollectorMarkLogic.ENTITY_NAME, "Person");
		runner.setProperty(EvaluateCollectorMarkLogic.FLOW_NAME, "default");
		runner.setProperty(EvaluateCollectorMarkLogic.BATCH_SIZE, "4");
		runner.setProperty(EvaluateCollectorMarkLogic.DHF_VERSION, "4");
		runner.setProperty(EvaluateCollectorMarkLogic.OPTIONS, "{\"entity\":\"Person\", \"flow\":\"default\", \"flowType\":\"harmonize\"}");
		
		runner.run();

		List<MockFlowFile> list = runner.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals(3, list.size(), "For 10 docs and a batch size of 4, 3 FlowFiles should have been created");
	}

	@Test
	public void scriptWithOptions() {
		TestRunner runner = getNewTestRunner(EvaluateCollectorMarkLogic.class);
		runner.setValidateExpressionUsage(false);
		runner.setProperty(EvaluateCollectorMarkLogic.ENTITY_NAME, "Person");
		runner.setProperty(EvaluateCollectorMarkLogic.FLOW_NAME, "default");
		runner.setProperty(EvaluateCollectorMarkLogic.BATCH_SIZE, "2");
		runner.setProperty(EvaluateCollectorMarkLogic.DHF_VERSION, "4");
		runner.setProperty(EvaluateCollectorMarkLogic.OPTIONS, "{\"entity\":\"Person\", \"flow\":\"default\", \"flowType\":\"harmonize\", \"dhf.limit\":3}");

		runner.run();

		List<MockFlowFile> list = runner.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals(2, list.size(), "The options should have limited the collector to 3 URIs, and with a batch size of 2, should have created 2 FlowFiles");
	}
	
	@Test
	public void customXqueryScript1() {
		TestRunner runner = getNewTestRunner(EvaluateCollectorMarkLogic.class);
		runner.setValidateExpressionUsage(false);
		runner.setProperty(EvaluateCollectorMarkLogic.BATCH_SIZE, "4");
		runner.setProperty(EvaluateCollectorMarkLogic.SCRIPT_TYPE, EvaluateCollectorMarkLogic.SCRIPT_TYPE_JAVASCRIPT);
		runner.setProperty(EvaluateCollectorMarkLogic.SCRIPT_BODY,
			"const contentPlugin = require('/entities/Person/harmonize/default/collector.sjs');  contentPlugin.collect();");

		runner.run();

		List<MockFlowFile> list = runner.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals(3, list.size());
	}
	
	@Test
	public void customXqueryScript2() {
		TestRunner runner = getNewTestRunner(EvaluateCollectorMarkLogic.class);
		runner.setValidateExpressionUsage(false);
		runner.setProperty(EvaluateCollectorMarkLogic.BATCH_SIZE, "4");
		runner.setProperty(EvaluateCollectorMarkLogic.SCRIPT_TYPE, EvaluateCollectorMarkLogic.SCRIPT_TYPE_XQUERY);
		runner.setProperty(EvaluateCollectorMarkLogic.SCRIPT_BODY,
			"cts:uris((), (), cts:collection-query('EVALUATE-COLLECTOR-TEST'))[1 to 4]");

		runner.run();

		List<MockFlowFile> list = runner.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals(1, list.size());
	}
	
}
