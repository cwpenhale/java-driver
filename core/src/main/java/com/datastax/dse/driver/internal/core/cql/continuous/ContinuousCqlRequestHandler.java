/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.internal.core.cql.continuous;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.dse.driver.internal.core.cql.DseConversions;
import com.datastax.dse.protocol.internal.response.result.DseRowsMetadata;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.DefaultRow;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.result.Rows;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import net.jcip.annotations.ThreadSafe;

/**
 * Handles a request that supports multiple response messages (a.k.a. continuous paging request).
 */
@ThreadSafe
public class ContinuousCqlRequestHandler
    extends ContinuousRequestHandlerBase<Statement, ContinuousAsyncResultSet> {

  private final Message message;
  private final Duration firstPageTimeout;
  private final Duration otherPagesTimeout;
  private final int maxEnqueuedPages;
  private final int maxPages;

  ContinuousCqlRequestHandler(
      @NonNull Statement<?> statement,
      @NonNull DefaultSession session,
      @NonNull InternalDriverContext context,
      @NonNull String sessionLogPrefix) {
    super(
        statement,
        session,
        context,
        sessionLogPrefix,
        ContinuousAsyncResultSet.class,
        false,
        DefaultSessionMetric.CQL_CLIENT_TIMEOUTS,
        DseSessionMetric.CONTINUOUS_CQL_REQUESTS,
        DefaultNodeMetric.CQL_MESSAGES);
    message = DseConversions.toContinuousPagingMessage(statement, executionProfile, context);
    throttler.register(this);
    firstPageTimeout =
        executionProfile.getDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE);
    otherPagesTimeout =
        executionProfile.getDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES);
    maxEnqueuedPages =
        executionProfile.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES);
    maxPages = executionProfile.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES);
  }

  @NonNull
  @Override
  protected Duration getGlobalTimeout() {
    return Duration.ZERO;
  }

  @NonNull
  @Override
  protected Duration getPageTimeout(int pageNumber) {
    return pageNumber == 1 ? firstPageTimeout : otherPagesTimeout;
  }

  @NonNull
  @Override
  protected Duration getReviseRequestTimeout() {
    return otherPagesTimeout;
  }

  @Override
  protected int getMaxEnqueuedPages() {
    return maxEnqueuedPages;
  }

  @Override
  protected int getMaxPages() {
    return maxPages;
  }

  @NonNull
  @Override
  protected Message getMessage() {
    return message;
  }

  @Override
  protected boolean isTracingEnabled() {
    return false;
  }

  @NonNull
  @Override
  protected Map<String, ByteBuffer> createPayload() {
    return statement.getCustomPayload();
  }

  @NonNull
  @Override
  protected ContinuousAsyncResultSet createEmptyResultSet(@NonNull ExecutionInfo executionInfo) {
    return DefaultContinuousAsyncResultSet.empty(executionInfo);
  }

  @NonNull
  @Override
  protected DefaultContinuousAsyncResultSet createResultSet(
      @NonNull Rows rows,
      @NonNull ExecutionInfo executionInfo,
      @NonNull ColumnDefinitions columnDefinitions) {
    Queue<List<ByteBuffer>> data = rows.getData();
    CountingIterator<Row> iterator =
        new CountingIterator<Row>(data.size()) {
          @Override
          protected Row computeNext() {
            List<ByteBuffer> rowData = data.poll();
            return (rowData == null)
                ? endOfData()
                : new DefaultRow(columnDefinitions, rowData, context);
          }
        };
    DseRowsMetadata metadata = (DseRowsMetadata) rows.getMetadata();
    return new DefaultContinuousAsyncResultSet(
        iterator,
        columnDefinitions,
        metadata.continuousPageNumber,
        !metadata.isLastContinuousPage,
        executionInfo,
        this);
  }

  @Override
  protected int pageNumber(@NonNull ContinuousAsyncResultSet resultSet) {
    return resultSet.pageNumber();
  }
}