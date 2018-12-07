/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.marklogic.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.ExportListener;
import com.marklogic.client.datamovement.JobReport;
import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatchListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.datamovement.impl.JobReportImpl;
import com.marklogic.client.document.DocumentManager.Metadata;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.RawStructuredQueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryDefinition;

@Tags({ "MarkLogic", "Get", "Query", "Read" })
@InputRequirement(Requirement.INPUT_ALLOWED)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@CapabilityDescription("Creates FlowFiles from batches of documents, matching the given criteria,"
        + " retrieved from a MarkLogic server using the MarkLogic Data Movement SDK (DMSDK)")
@DynamicProperty(name = "Server transform parameter name", value = "Value of the server transform parameter",
description = "Adds server transform parameters to be passed to the server transform specified. "
+ "Server transform parameter name should start with the string 'trans:'.",
expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename is set to the uri of the document retrieved from MarkLogic") })
public class QueryMarkLogic extends AbstractMarkLogicProcessor {

    public static final PropertyDescriptor CONSISTENT_SNAPSHOT = new PropertyDescriptor.Builder()
            .name("Consistent Snapshot").displayName("Consistent Snapshot").defaultValue("true")
            .description("Boolean used to indicate that the matching documents were retrieved from a "
                    + "consistent snapshot")
            .required(true).addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder()
            .name("Return Type").displayName("Return Type").defaultValue(ReturnTypes.DOCUMENTS.getValue())
            .description("Determines what gets retrieved by query").required(true)
            .allowableValues(ReturnTypes.allValues)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder().name("Query").displayName("Query")
            .description("Query text that corresponds with the selected Query Type").required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor QUERY_TYPE = new PropertyDescriptor.Builder().name("Query Type")
            .displayName("Query Type").description("Type of query that will be used to retrieve data from MarkLogic")
            .required(true).allowableValues(QueryTypes.allValues).defaultValue(QueryTypes.COMBINED_JSON.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor COLLECTIONS = new PropertyDescriptor.Builder().name("Collections")
            .displayName("Collections")
            .description(
                    "**Deprecated: Use Query Type and Query** Comma-separated list of collections to query from a MarkLogic server")
            .required(false).addValidator(Validator.VALID).build();

    protected static final Relationship SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are created from documents read from MarkLogic are routed to"
                    + " this success relationship.")
            .build();

    protected static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that failed to produce a valid query.")
            .build();

    private QueryBatcher queryBatcher;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .addValidator(Validator.VALID)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .build();
    }

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);

        List<PropertyDescriptor> list = new ArrayList<>(properties);
        list.add(CONSISTENT_SNAPSHOT);
        list.add(QUERY);
        list.add(QUERY_TYPE);
        list.add(RETURN_TYPE);
        list.add(TRANSFORM);
        list.add(COLLECTIONS);
        properties = Collections.unmodifiableList(list);
        Set<Relationship> set = new HashSet<>();
        set.add(SUCCESS);
        set.add(FAILURE);
        relationships = Collections.unmodifiableSet(set);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        Set<ValidationResult> validationResultSet = new HashSet<>();
        String collections = validationContext.getProperty(COLLECTIONS).getValue();
        String query = validationContext.getProperty(QUERY).getValue();
        if (collections == null && query == null) {
            validationResultSet.add(new ValidationResult.Builder().subject("Query").valid(false)
                    .explanation("The Query value must be set. "
                            + "The deprecated Collections property will be migrated appropriately.")
                    .build());
        }
        return validationResultSet;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory)
            throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        onTrigger(context, session);
    }

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        try {
            FlowFile input = null;

            if (context.hasIncomingConnection()) {
                input = session.get();
            }

            DataMovementManager dataMovementManager = getDatabaseClient(context).newDataMovementManager();
            queryBatcher = createQueryBatcherWithQueryCriteria(context, session, input, getDatabaseClient(context),
                    dataMovementManager);
            if (context.getProperty(BATCH_SIZE).asInteger() != null)
                queryBatcher.withBatchSize(context.getProperty(BATCH_SIZE).asInteger());
            if (context.getProperty(THREAD_COUNT).asInteger() != null)
                queryBatcher.withThreadCount(context.getProperty(THREAD_COUNT).asInteger());
            final boolean consistentSnapshot;
            if (context.getProperty(CONSISTENT_SNAPSHOT).asBoolean() != null
                    && !context.getProperty(CONSISTENT_SNAPSHOT).asBoolean()) {
                consistentSnapshot = false;
            } else {
                queryBatcher.withConsistentSnapshot();
                consistentSnapshot = true;
            }

            QueryBatchListener batchListener = buildQueryBatchListener(context, session, consistentSnapshot);
            queryBatcher.onJobCompletion((batcher) -> {
                JobReport report = new JobReportImpl(batcher);
                if (report.getSuccessEventsCount() == 0) {
                    context.yield();
                }
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("ML Query Job Complete [Success Count=" + report.getSuccessEventsCount() + "] [Failure Count=" + report.getFailureEventsCount() + "]");
                }
            });
            queryBatcher.onUrisReady(batchListener);
            dataMovementManager.startJob(queryBatcher);
            queryBatcher.awaitCompletion();
            dataMovementManager.stopJob(queryBatcher);
        } catch (final Throwable t) {
            session.rollback(true);
            context.yield();
            this.handleThrowable(t);
        }
    }

    protected QueryBatchListener buildQueryBatchListener(final ProcessContext context, final ProcessSession session, final boolean consistentSnapshot) {
        final boolean retrieveFullDocument;
        if (context.getProperty(RETURN_TYPE) != null
                && (ReturnTypes.DOCUMENTS_STR.equals(context.getProperty(RETURN_TYPE).getValue())
                        || ReturnTypes.DOCUMENTS_AND_META_STR.equals(context.getProperty(RETURN_TYPE).getValue()))) {
            retrieveFullDocument = true;
        } else {
            retrieveFullDocument = false;
        }
        final boolean retrieveMetadata;
        if (context.getProperty(RETURN_TYPE) != null
                && (ReturnTypes.META.getValue().equals(context.getProperty(RETURN_TYPE).getValue())
                        || ReturnTypes.DOCUMENTS_AND_META.getValue().equals(context.getProperty(RETURN_TYPE).getValue()))) {
            retrieveMetadata = true;
        } else {
            retrieveMetadata = false;
        }

        QueryBatchListener batchListener = null;

        if (retrieveFullDocument) {
            ExportListener exportListener = new ExportListener().onDocumentReady(doc -> {
                synchronized(session) {
                    final FlowFile flowFile = session.write(session.create(),
                            out -> out.write(doc.getContent(new BytesHandle()).get()));
                    if (retrieveMetadata) {
                        DocumentMetadataHandle metaHandle = doc.getMetadata(new DocumentMetadataHandle());
                        metaHandle.getMetadataValues().forEach((metaKey,metaValue) -> {
                            session.putAttribute(flowFile, "meta:" + metaKey, metaValue);
                        });
                        metaHandle.getProperties().forEach((qname,propertyValue) -> {
                            session.putAttribute(flowFile, "property:" + qname.toString(), propertyValue.toString());
                        });
                    }
                    session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), doc.getUri());
                    session.transfer(flowFile, SUCCESS);
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("Routing " + doc.getUri() + " to " + SUCCESS.getName());
                    }
                    session.commit();
                }
            });
            if (retrieveMetadata) {
                exportListener.withMetadataCategory(Metadata.ALL);
            }
            if (consistentSnapshot) {
                exportListener.withConsistentSnapshot();
            }
            ServerTransform transform = this.buildServerTransform(context);
            if (transform != null) {
                exportListener.withTransform(transform);
            }
            batchListener = exportListener;
        } else {
            batchListener = new QueryBatchListener() {
                @Override
                public void processEvent(QueryBatch batch) {
                    synchronized(session) {
                        Arrays.stream(batch.getItems()).forEach((uri) -> {
                            FlowFile flowFile = session.create();
                            session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), uri);
                            if (retrieveMetadata) {
                                DocumentMetadataHandle metaHandle = new DocumentMetadataHandle();
                                if (consistentSnapshot) {
                                    metaHandle.setServerTimestamp(batch.getServerTimestamp());
                                }
                                batch.getClient().newDocumentManager().readMetadata(uri, metaHandle);
                                metaHandle.getMetadataValues().forEach((metaKey,metaValue) -> {
                                    session.putAttribute(flowFile, "meta:" + metaKey, metaValue);
                                });
                                metaHandle.getProperties().forEach((qname,propertyValue) -> {
                                    session.putAttribute(flowFile, "property:" + qname.toString(), propertyValue.toString());
                                });
                            }
                            session.transfer(flowFile, SUCCESS);
                            if (getLogger().isDebugEnabled()) {
                                getLogger().debug("Routing " + uri + " to " + SUCCESS.getName());
                            }
                        });;
                        session.commit();
                    }
                }
            };
        }
        return batchListener;
    }

    private QueryBatcher createQueryBatcherWithQueryCriteria(ProcessContext context, ProcessSession session, FlowFile flowFile,
            DatabaseClient databaseClient, DataMovementManager dataMovementManager) {
        final PropertyValue queryProperty = context.getProperty(QUERY);
        final String queryValue;
        final String queryTypeValue;

        // Gracefully migrate old Collections templates
        String collectionsValue = context.getProperty(COLLECTIONS).getValue();
        if (!(collectionsValue == null || "".equals(collectionsValue))) {
            queryValue = collectionsValue;
            queryTypeValue = QueryTypes.COLLECTION.getValue();
        } else {
            queryValue = queryProperty.evaluateAttributeExpressions(flowFile).getValue();
            queryTypeValue = context.getProperty(QUERY_TYPE).getValue();
        }
        if (queryValue != null) {
            final QueryManager queryManager = databaseClient.newQueryManager();
            switch (queryTypeValue) {
                case QueryTypes.COLLECTION_STR:
                    String[] collections = getArrayFromCommaSeparatedString(queryValue);
                    if (collections != null) {
                        StructuredQueryDefinition query = queryManager.newStructuredQueryBuilder()
                                .collection(collections);
                        queryBatcher = dataMovementManager.newQueryBatcher(query);
                    }
                    break;
                case QueryTypes.COMBINED_JSON_STR:
                    queryBatcher = batcherFromCombinedQuery(dataMovementManager, queryManager, queryValue, Format.JSON);
                    break;
                case QueryTypes.COMBINED_XML_STR:
                    queryBatcher = batcherFromCombinedQuery(dataMovementManager, queryManager, queryValue, Format.XML);
                    break;
                case QueryTypes.STRING_STR:
                    StringQueryDefinition strDef = queryManager.newStringDefinition().withCriteria(queryValue);
                    queryBatcher = dataMovementManager.newQueryBatcher(strDef);
                    break;
                case QueryTypes.STRUCTURED_JSON_STR:
                    queryBatcher = batcherFromStructuredQuery(dataMovementManager, queryManager, queryValue, Format.JSON);
                    break;
                case QueryTypes.STRUCTURED_XML_STR:
                    queryBatcher = batcherFromStructuredQuery(dataMovementManager, queryManager, queryValue, Format.XML);
                    break;
                default:
                    throw new IllegalStateException("No valid Query type selected!");
            }
        }
        if (queryBatcher == null) {
            throw new IllegalStateException("No valid Query criteria specified!");
        }
        queryBatcher.onQueryFailure(exception -> {
            FlowFile failureFlowFile = null;
            if (flowFile != null) {
                failureFlowFile = session.penalize(flowFile);
            } else {
                failureFlowFile = session.create();
            }
            session.transfer(failureFlowFile, FAILURE);
            session.commit();
            getLogger().error("Query failure: " + exception.getMessage());
            context.yield();
        });
        return queryBatcher;
    }

    QueryBatcher getQueryBatcher() {
        return this.queryBatcher;
    }

    StringHandle handleForQuery(String queryValue, Format format) {
        StringHandle handle = new StringHandle().withFormat(format).with(queryValue);
        return handle;
    }

    QueryBatcher batcherFromCombinedQuery(DataMovementManager dataMovementManager, QueryManager queryMgr,
            String queryValue, Format format) {
        RawCombinedQueryDefinition qdef = queryMgr.newRawCombinedQueryDefinition(handleForQuery(queryValue, format));
        return dataMovementManager.newQueryBatcher(qdef);
    }

    QueryBatcher batcherFromStructuredQuery(DataMovementManager dataMovementManager, QueryManager queryMgr,
            String queryValue, Format format) {
        RawStructuredQueryDefinition qdef = queryMgr
                .newRawStructuredQueryDefinition(handleForQuery(queryValue, format));
        return dataMovementManager.newQueryBatcher(qdef);
    }

    public static class QueryTypes extends AllowableValuesSet {
        public static final String COLLECTION_STR = "Collection Query";
        public static final AllowableValue COLLECTION = new AllowableValue(COLLECTION_STR, COLLECTION_STR,
                "Comma-separated list of collections to query from a MarkLogic server");
        public static final String COMBINED_JSON_STR = "Combined Query (JSON)";
        public static final AllowableValue COMBINED_JSON = new AllowableValue(COMBINED_JSON_STR, COMBINED_JSON_STR,
                "Combine a string or structured query with dynamic query options (Allows JSON serialized cts queries)");
        public static final String COMBINED_XML_STR = "Combined Query (XML)";
        public static final AllowableValue COMBINED_XML = new AllowableValue(COMBINED_XML_STR, COMBINED_XML_STR,
                "Combine a string or structured query with dynamic query options (Allows XML serialized cts queries)");
        public static final String STRING_STR = "String Query";
        public static final AllowableValue STRING = new AllowableValue(STRING_STR, STRING_STR,
                "A Google-style query string to search documents and metadata.");
        public static final String STRUCTURED_JSON_STR = "Structured Query (JSON)";
        public static final AllowableValue STRUCTURED_JSON = new AllowableValue(STRUCTURED_JSON_STR, STRUCTURED_JSON_STR,
                "A simple and easy way to construct queries as a JSON structure, allowing you to manipulate complex queries");
        public static final String STRUCTURED_XML_STR = "Structured Query (XML)";
        public static final AllowableValue STRUCTURED_XML = new AllowableValue(STRUCTURED_XML_STR, STRUCTURED_XML_STR,
                "A simple and easy way to construct queries as a XML structure, allowing you to manipulate complex queries");

        public static final AllowableValue[] allValues = new AllowableValue[] { COLLECTION, COMBINED_JSON, COMBINED_XML, STRING, STRUCTURED_JSON, STRUCTURED_XML };

    }

    public static class ReturnTypes extends AllowableValuesSet {
        public static final String URIS_ONLY_STR = "URIs Only";
        public static final AllowableValue URIS_ONLY = new AllowableValue(URIS_ONLY_STR, URIS_ONLY_STR,
                "Only return document URIs for matching documents in FlowFile attribute");
        public static final String DOCUMENTS_STR = "Documents";
        public static final AllowableValue DOCUMENTS = new AllowableValue(DOCUMENTS_STR, DOCUMENTS_STR,
                "Return documents in FlowFile contents");
        public static final String DOCUMENTS_AND_META_STR = "Documents + Metadata";
        public static final AllowableValue DOCUMENTS_AND_META = new AllowableValue(DOCUMENTS_AND_META_STR, DOCUMENTS_AND_META_STR,
                "Return documents in FlowFile contents and metadata in FlowFile attributes");
        public static final String META_STR = "Metadata";
        public static final AllowableValue META = new AllowableValue(META_STR, META_STR,
                "Return metadata in FlowFile attributes");

        public static final AllowableValue[] allValues = new AllowableValue[] { URIS_ONLY, DOCUMENTS, DOCUMENTS_AND_META, META };

    }
}