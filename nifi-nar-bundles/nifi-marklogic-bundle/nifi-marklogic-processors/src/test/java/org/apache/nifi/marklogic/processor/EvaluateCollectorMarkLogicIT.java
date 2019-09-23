package org.apache.nifi.marklogic.processor;

import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.StringHandle;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This just focuses on evaluating the collector script against MarkLogic, as that can't be done in
 * EvaluateCollectorMarkLogicTest.
 */
public class EvaluateCollectorMarkLogicIT extends AbstractMarkLogicIT {

	@BeforeEach
	public void setup() {
		JSONDocumentManager mgr = getDatabaseClient().newJSONDocumentManager();
		for (int i = 0; i < 5; i++) {
			mgr.write("/test/" + i + ".json", new StringHandle(format("{\"test\":\"%d\"}", i)));
		}

		batchSize = "2";
	}

	@Test
	public void test() {
		TestRunner runner = getNewTestRunner(EvaluateCollectorMarkLogic.class);
		runner.setProperty(EvaluateCollectorMarkLogic.ENTITY_NAME, "person");
		runner.setProperty(EvaluateCollectorMarkLogic.FLOW_NAME, "default");

		runner.run();

		List<MockFlowFile> list = runner.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals(3, list.size(), "For 5 docs and a batch size of 2, 3 FlowFiles should have been created");
	}

	@Test
	public void customXqueryScript() {
		TestRunner runner = getNewTestRunner(EvaluateCollectorMarkLogic.class);
		runner.setProperty(EvaluateCollectorMarkLogic.SCRIPT_TYPE, EvaluateCollectorMarkLogic.SCRIPT_TYPE_XQUERY);
		runner.setProperty(EvaluateCollectorMarkLogic.SCRIPT_BODY,
			"import module namespace p = 'http://marklogic.com/data-hub/plugins' at '/entities/person/harmonize/default/collector.xqy'; p:collect(map:map())");

		runner.run();

		List<MockFlowFile> list = runner.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals(3, list.size());
	}

	@Test
	public void scriptWithOptions() {
		TestRunner runner = getNewTestRunner(EvaluateCollectorMarkLogic.class);
		runner.setProperty(EvaluateCollectorMarkLogic.ENTITY_NAME, "person");
		runner.setProperty(EvaluateCollectorMarkLogic.FLOW_NAME, "default");
		runner.setProperty(EvaluateCollectorMarkLogic.OPTIONS, "{\"dhf.limit\":3}");

		runner.run();

		List<MockFlowFile> list = runner.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals(2, list.size(), "The options should have limited the collector to 3 URIs, and with a batch size of 2, should have created 2 FlowFiles");
	}
}
