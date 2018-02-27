/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.couchbase2;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultScheduledPollConsumer;

import static org.apache.camel.component.couchbase2.CouchbaseConstants.HEADER_DESIGN_DOCUMENT_NAME;
import static org.apache.camel.component.couchbase2.CouchbaseConstants.HEADER_ID;
import static org.apache.camel.component.couchbase2.CouchbaseConstants.HEADER_KEY;
import static org.apache.camel.component.couchbase2.CouchbaseConstants.HEADER_VIEWNAME;

public class CouchbaseConsumer extends DefaultScheduledPollConsumer {

    private final CouchbaseEndpoint endpoint;
    private final Bucket couchbaseBucket;
    private final ViewQuery query;

    public CouchbaseConsumer(CouchbaseEndpoint endpoint, Bucket couchbaseBucket, Processor processor) {

        super(endpoint, processor);
        this.couchbaseBucket = couchbaseBucket;
        this.endpoint = endpoint;
        this.query = ViewQuery.from(endpoint.getDesignDocumentName(), endpoint.getViewName());
        init();

    }

    private void init() {

        query.includeDocs(true);
        
        int limit = endpoint.getLimit();
        if (limit > 0) {
            query.limit(limit);
        }

        int skip = endpoint.getSkip();
        if (skip > 0) {
            query.skip(skip);
        }

        query.descending(endpoint.isDescending());

        String rangeStartKey = endpoint.getRangeStartKey();
        String rangeEndKey = endpoint.getRangeEndKey();
        if ("".equals(rangeStartKey) || "".equals(rangeEndKey)) {
            return;
        }
        query.startKey(rangeStartKey);
        query.endKey(rangeEndKey);

    }

    @Override
    protected void doStart() throws Exception {
        log.info("Starting Couchbase consumer");
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        log.info("Stopping Couchbase consumer");
        super.doStop();
        if (couchbaseBucket != null) {
            couchbaseBucket.close();
        }
    }

    @Override
    protected synchronized int poll() throws Exception {
    	ViewResult result = couchbaseBucket.query(query);
        log.info("Received result set from Couchbase");

        if (log.isTraceEnabled()) {
            log.trace("ViewResponse = {}", result);
        }

        String consumerProcessedStrategy = endpoint.getConsumerProcessedStrategy();

        for (ViewRow row : result) {

            String id = row.id();
            Object doc = row.document();

            String key = row.key().toString();
            String designDocumentName = endpoint.getDesignDocumentName();
            String viewName = endpoint.getViewName();

            Exchange exchange = endpoint.createExchange();
            exchange.getIn().setBody(doc);
            exchange.getIn().setHeader(HEADER_ID, id);
            exchange.getIn().setHeader(HEADER_KEY, key);
            exchange.getIn().setHeader(HEADER_DESIGN_DOCUMENT_NAME, designDocumentName);
            exchange.getIn().setHeader(HEADER_VIEWNAME, viewName);

            if ("delete".equalsIgnoreCase(consumerProcessedStrategy)) {
                if (log.isTraceEnabled()) {
                    log.trace("Deleting doc with ID {}", id);
                }
                couchbaseBucket.remove(id);
            } else if ("filter".equalsIgnoreCase(consumerProcessedStrategy)) {
                if (log.isTraceEnabled()) {
                    log.trace("Filtering out ID {}", id);
                }
                // add filter for already processed docs
            } else {
                log.trace("No strategy set for already processed docs, beware of duplicates!");
            }

            logDetails(id, doc, key, designDocumentName, viewName, exchange);

            try {
                this.getProcessor().process(exchange);
            } catch (Exception e) {
                this.getExceptionHandler().handleException("Error processing exchange.", exchange, e);
            }
        }

        return result.totalRows();
    }

    private void logDetails(String id, Object doc, String key, String designDocumentName, String viewName, Exchange exchange) {

        if (log.isTraceEnabled()) {
            log.trace("Created exchange = {}", exchange);
            log.trace("Added Document in body = {}", doc);
            log.trace("Adding to Header");
            log.trace("ID = {}", id);
            log.trace("Key = {}", key);
            log.trace("Design Document Name = {}", designDocumentName);
            log.trace("View Name = {}", viewName);
        }

    }
}
