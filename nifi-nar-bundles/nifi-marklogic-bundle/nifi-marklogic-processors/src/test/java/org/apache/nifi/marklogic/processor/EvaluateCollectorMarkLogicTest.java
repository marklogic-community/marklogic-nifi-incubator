package org.apache.nifi.marklogic.processor;

import com.marklogic.client.eval.EvalResult;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EvaluateCollectorMarkLogicTest extends AbstractMarkLogicProcessorTest {

	private EvaluateCollectorMarkLogic myProcessor;

	@Before
	public void setup() {
		myProcessor = new EvaluateCollectorMarkLogic();
		initialize(myProcessor);
	}

	@Test
	public void defaultScript() {
		processContext.setProperty(EvaluateCollectorMarkLogic.ENTITY_NAME, "person");
		processContext.setProperty(EvaluateCollectorMarkLogic.FLOW_NAME, "default");
		assertEquals("var options; const collector = require('/entities/person/harmonize/default/collector.sjs'); collector.collect(options)",
			myProcessor.buildScriptToEvaluate(processContext, addTestFlowFile()));
	}

	@Test
	public void customScript() {
		processContext.setProperty(EvaluateCollectorMarkLogic.SCRIPT_BODY, "thisCanBeAnything()");
		assertEquals("thisCanBeAnything()", myProcessor.buildScriptToEvaluate(processContext, addTestFlowFile()));
	}

	@Test
	public void entityAndFlowNamesEvaluatedAgainstFlowFile() {
		processContext.setProperty(EvaluateCollectorMarkLogic.ENTITY_NAME, "${theEntityName}");
		processContext.setProperty(EvaluateCollectorMarkLogic.FLOW_NAME, "${theFlowName}");

		Map<String, String> attributes = new HashMap<>();
		attributes.put("theEntityName", "someEntity");
		attributes.put("theFlowName", "someFlow");

		assertEquals("var options; const collector = require('/entities/someEntity/harmonize/someFlow/collector.sjs'); collector.collect(options)",
			myProcessor.buildScriptToEvaluate(processContext, addFlowFile(attributes, "test")));
	}

	@Test
	public void createFlowFilesWithDefaults() {
		processContext.setProperty(EvaluateCollectorMarkLogic.BATCH_SIZE, "2");

		myProcessor.createFlowFilesForBatchesOfIdentifiers(processContext, processSession, addTestFlowFile(), buildMockResults());

		List<MockFlowFile> newFlowFiles = processSession.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals("There should be 2 flow files, since our fake iterator had 3 ids in it, and the batch size is set to 2", 2, newFlowFiles.size());

		MockFlowFile firstBatch = newFlowFiles.get(0);
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id1,id2");
		firstBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "1");
		firstBatch.assertAttributeNotExists(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE);

		MockFlowFile secondBatch = newFlowFiles.get(1);
		secondBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id3");
		secondBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "2");
		secondBatch.assertAttributeNotExists(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE);
	}

	@Test
	public void createFlowFilesWithFragmentCountSet() {
		processContext.setProperty(EvaluateCollectorMarkLogic.SET_FRAGMENT_COUNT, "true");
		processContext.setProperty(EvaluateCollectorMarkLogic.BATCH_SIZE, "2");

		myProcessor.createFlowFilesForBatchesOfIdentifiers(processContext, processSession, addTestFlowFile(), buildMockResults());

		List<MockFlowFile> newFlowFiles = processSession.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals(2, newFlowFiles.size());

		MockFlowFile firstBatch = newFlowFiles.get(0);
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id1,id2");
		firstBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "1");
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE, "3");

		MockFlowFile secondBatch = newFlowFiles.get(1);
		secondBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id3");
		secondBatch.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "2");
		secondBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_TOTAL_ATTRIBUTE, "3");
	}

	@Test
	public void customDelimiter() {
		processContext.setProperty(EvaluateCollectorMarkLogic.DELIMITER, ":");

		myProcessor.createFlowFilesForBatchesOfIdentifiers(processContext, processSession, addTestFlowFile(), buildMockResults());

		List<MockFlowFile> newFlowFiles = processSession.getFlowFilesForRelationship(EvaluateCollectorMarkLogic.BATCHES);
		assertEquals("The batch size defaults to 100, and with 3 fake results, we should have one batch FlowFile", 1, newFlowFiles.size());

		MockFlowFile firstBatch = newFlowFiles.get(0);
		firstBatch.assertAttributeEquals(EvaluateCollectorMarkLogic.IDENTIFIERS_ATTRIBUTE, "id1:id2:id3");
	}

	private MockEvalResultIterator buildMockResults() {
		List<EvalResult> results = new ArrayList<>();
		results.add(new StringEvalResult("id1"));
		results.add(new StringEvalResult("id2"));
		results.add(new StringEvalResult("id3"));
		return new MockEvalResultIterator(results);
	}
}
