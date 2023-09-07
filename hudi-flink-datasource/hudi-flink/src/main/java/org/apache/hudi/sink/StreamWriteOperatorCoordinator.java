/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink;

import org.apache.hudi.adapter.OperatorCoordinatorAdapter;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.sink.event.CommitAckEvent;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.meta.CkpMetadata;
import org.apache.hudi.sink.utils.HiveSyncContext;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.util.ClientIds;
import org.apache.hudi.util.ClusteringUtil;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.hudi.util.StreamerUtil.initTableIfNotExists;

/**
 * {@link OperatorCoordinator} for {@link StreamWriteFunction}.
 *
 * <p>This coordinator starts a new instant when a new checkpoint starts. It commits the instant when all the
 * operator tasks write the buffer successfully for a round of checkpoint.
 *
 * <p>If there is no data for a round of checkpointing, it resets the events buffer and returns early.
 *
 * @see StreamWriteFunction for the work flow and semantics
 */
public class StreamWriteOperatorCoordinator
    implements OperatorCoordinatorAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(StreamWriteOperatorCoordinator.class);

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Hive config options.
   */
  private final SerializableConfiguration hiveConf;

  /**
   * Coordinator context.
   */
  private final Context context;

  /**
   * Gateways for sending events to sub tasks.
   */
  private transient SubtaskGateway[] gateways;

  /**
   * Write client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  /**
   * Meta client.
   */
  private transient HoodieTableMetaClient metaClient;

  /**
   * Current REQUESTED instant, for validation.
   */
  private volatile String instant = WriteMetadataEvent.BOOTSTRAP_INSTANT;

  /**
   * Event buffer for one round of checkpointing. When all the elements are non-null and have the same
   * write instant, then the instant succeed and we can commit it.
   */
  private transient WriteMetadataEvent[] eventBuffer;

  /**
   * Task number of the operator.
   */
  private final int parallelism;

  /**
   * A single-thread executor to handle all the asynchronous jobs of the coordinator.
   */
  private NonThrownExecutor executor;

  /**
   * A single-thread executor to handle asynchronous hive sync.
   */
  private NonThrownExecutor hiveSyncExecutor;

  /**
   * Context that holds variables for asynchronous hive sync.
   */
  private HiveSyncContext hiveSyncContext;

  /**
   * The table state.
   */
  private transient TableState tableState;

  /**
   * The checkpoint metadata.
   */
  private CkpMetadata ckpMetadata;

  /**
   * The client id heartbeats.
   */
  private ClientIds clientIds;

  /**
   * Constructs a StreamingSinkOperatorCoordinator.
   *
   * @param conf    The config options
   * @param context The coordinator context
   */
  public StreamWriteOperatorCoordinator(
      Configuration conf,
      Context context) {
    this.conf = conf;
    this.context = context;
    this.parallelism = context.currentParallelism();
    this.hiveConf = new SerializableConfiguration(HadoopConfigurations.getHiveConf(conf));
  }

  @Override
  public void start() throws Exception {
    // setup classloader for APIs that use reflection without taking ClassLoader param
    // reference: https://stackoverflow.com/questions/1771679/difference-between-threads-context-class-loader-and-normal-classloader
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    // initialize event buffer
    reset();
    this.gateways = new SubtaskGateway[this.parallelism];
    // init table, create if not exists.
    this.metaClient = initTableIfNotExists(this.conf);
    this.ckpMetadata = initCkpMetadata(this.metaClient, this.conf);
    // the write client must create after the table creation
    this.writeClient = FlinkWriteClients.createWriteClient(conf);
    initMetadataTable(this.writeClient);
    this.tableState = TableState.create(conf);
    // start the executor
    this.executor = NonThrownExecutor.builder(LOG)
        .exceptionHook((errMsg, t) -> this.context.failJob(new HoodieException(errMsg, t)))
        .waitForTasksFinish(true).build();
    // start the executor if required
    if (tableState.syncHive) {
      initHiveSync();
    }
    // start client id heartbeats for optimistic concurrency control
    if (OptionsResolver.isOptimisticConcurrencyControl(conf)) {
      initClientIds(conf);
    }
  }

  @Override
  public void close() throws Exception {
    // teardown the resource
    if (executor != null) {
      executor.close();
    }
    if (hiveSyncExecutor != null) {
      hiveSyncExecutor.close();
    }
    // the write client must close after the executor service
    // because the task in the service may send requests to the embedded timeline service.
    if (writeClient != null) {
      writeClient.close();
    }
    this.eventBuffer = null;
    if (this.ckpMetadata != null) {
      this.ckpMetadata.close();
    }
    if (this.clientIds != null) {
      this.clientIds.close();
    }
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
    executor.execute(
        () -> {
          try {
            result.complete(new byte[0]);
          } catch (Throwable throwable) {
            // when a checkpoint fails, throws directly.
            result.completeExceptionally(
                new CompletionException(
                    String.format("Failed to checkpoint Instant %s for source %s",
                        this.instant, this.getClass().getSimpleName()), throwable));
          }
        }, "taking checkpoint %d", checkpointId
    );
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    executor.execute(
        () -> {
          // The executor thread inherits the classloader of the #notifyCheckpointComplete
          // caller, which is a AppClassLoader.
          Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
          // for streaming mode, commits the ever received events anyway,
          // the stream write task snapshot and flush the data buffer synchronously in sequence,
          // so a successful checkpoint subsumes the old one(follows the checkpoint subsuming contract)
          final boolean committed = commitInstant(this.instant, checkpointId);
          // schedules the compaction or clustering if it is enabled in stream execution mode
          scheduleTableServices(committed);

          if (committed) {
            // start new instant.
            startInstant();
            // sync Hive if is enabled
            syncHiveAsync();
          }
        }, "commits the instant %s", this.instant
    );
  }

  @Override
  public void resetToCheckpoint(long checkpointID, byte[] checkpointData) {
    // no operation
  }

  @Override
  public void handleEventFromOperator(int i, OperatorEvent operatorEvent) {
    ValidationUtils.checkState(operatorEvent instanceof WriteMetadataEvent,
        "The coordinator can only handle WriteMetaEvent");
    WriteMetadataEvent event = (WriteMetadataEvent) operatorEvent;

    if (event.isEndInput()) {
      // handle end input event synchronously
      // wrap handleEndInputEvent in executeSync to preserve the order of events
      executor.executeSync(() -> handleEndInputEvent(event), "handle end input event for instant %s", this.instant);
    } else {
      executor.execute(
          () -> {
            if (event.isBootstrap()) {
              handleBootstrapEvent(event);
            } else {
              handleWriteMetaEvent(event);
            }
          }, "handle write metadata event for instant %s", this.instant
      );
    }
  }

  @Override
  public void subtaskFailed(int i, @Nullable Throwable throwable) {
    // reset the event
    this.eventBuffer[i] = null;
    LOG.warn("Reset the event for task [" + i + "]", throwable);
  }

  @Override
  public void subtaskReset(int i, long l) {
    // no operation
  }

  @Override
  public void subtaskReady(int i, SubtaskGateway subtaskGateway) {
    this.gateways[i] = subtaskGateway;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void initHiveSync() {
    this.hiveSyncExecutor = NonThrownExecutor.builder(LOG).waitForTasksFinish(true).build();
    this.hiveSyncContext = HiveSyncContext.create(conf, this.hiveConf);
  }

  private void syncHiveAsync() {
    if (tableState.syncHive) {
      this.hiveSyncExecutor.execute(this::doSyncHive, "sync hive metadata for instant %s", this.instant);
    }
  }

  private void syncHive() {
    if (tableState.syncHive) {
      doSyncHive();
      LOG.info("Sync hive metadata for instant {} success!", this.instant);
    }
  }

  /**
   * Sync hoodie table metadata to Hive metastore.
   */
  public void doSyncHive() {
    try (HiveSyncTool syncTool = hiveSyncContext.hiveSyncTool()) {
      syncTool.syncHoodieTable();
    }
  }

  private static void initMetadataTable(HoodieFlinkWriteClient<?> writeClient) {
    writeClient.initMetadataTable();
  }

  private static CkpMetadata initCkpMetadata(HoodieTableMetaClient metaClient, Configuration conf) throws IOException {
    CkpMetadata ckpMetadata = CkpMetadata.getInstance(metaClient, conf.getString(FlinkOptions.WRITE_CLIENT_ID));
    ckpMetadata.bootstrap();
    return ckpMetadata;
  }

  private void initClientIds(Configuration conf) {
    this.clientIds = ClientIds.builder().conf(conf).build();
    this.clientIds.start();
  }

  private void reset() {
    this.eventBuffer = new WriteMetadataEvent[this.parallelism];
  }

  /**
   * Checks the buffer is ready to commit.
   */
  private boolean allEventsReceived() {
    return Arrays.stream(eventBuffer)
        // we do not use event.isReady to check the instant
        // because the write task may send an event eagerly for empty
        // data set, the even may have a timestamp of last committed instant.
        .allMatch(event -> event != null && event.isLastBatch());
  }

  private void addEventToBuffer(WriteMetadataEvent event) {
    if (this.eventBuffer[event.getTaskID()] != null) {
      this.eventBuffer[event.getTaskID()].mergeWith(event);
    } else {
      this.eventBuffer[event.getTaskID()] = event;
    }
  }

  private void startInstant() {
    // refresh the last txn metadata
    this.writeClient.preTxn(this.metaClient);
    // put the assignment in front of metadata generation,
    // because the instant request from write task is asynchronous.
    this.instant = this.writeClient.startCommit(tableState.commitAction, this.metaClient);
    this.writeClient.setWriteTimer(tableState.commitAction);
    this.metaClient.getActiveTimeline().transitionRequestedToInflight(tableState.commitAction, this.instant);
    this.ckpMetadata.startInstant(this.instant);
    LOG.info("Create instant [{}] for table [{}] with type [{}]", this.instant,
        this.conf.getString(FlinkOptions.TABLE_NAME), conf.getString(FlinkOptions.TABLE_TYPE));
  }

  /**
   * Initializes the instant.
   *
   * <p>Recommits the last inflight instant if the write metadata checkpoint successfully
   * but was not committed due to some rare cases.
   *
   * <p>Starts a new instant, a writer can not flush data buffer
   * until it finds a new inflight instant on the timeline.
   */
  private void initInstant(String instant) {
    HoodieTimeline completedTimeline = this.metaClient.getActiveTimeline().filterCompletedInstants();
    executor.execute(() -> {
      if (instant.equals(WriteMetadataEvent.BOOTSTRAP_INSTANT) || completedTimeline.containsInstant(instant)) {
        // the last instant committed successfully
        reset();
      } else {
        LOG.info("Recommit instant {}", instant);
        // Recommit should start heartbeat for lazy failed writes clean policy to avoid aborting for heartbeat expired.
        if (writeClient.getConfig().getFailedWritesCleanPolicy().isLazy()) {
          writeClient.getHeartbeatClient().start(instant);
        }
        commitInstant(instant);
      }
      // starts a new instant
      startInstant();
      // upgrade downgrade
      this.writeClient.upgradeDowngrade(this.instant, this.metaClient);
    }, "initialize instant %s", instant);
  }

  private void handleBootstrapEvent(WriteMetadataEvent event) {
    this.eventBuffer[event.getTaskID()] = event;
    if (Arrays.stream(eventBuffer).allMatch(evt -> evt != null && evt.isBootstrap())) {
      // start to initialize the instant.
      final String instant = Arrays.stream(eventBuffer)
          .filter(evt -> evt.getWriteStatuses().size() > 0)
          .findFirst().map(WriteMetadataEvent::getInstantTime)
          .orElse(WriteMetadataEvent.BOOTSTRAP_INSTANT);
      initInstant(instant);
    }
  }

  private void handleEndInputEvent(WriteMetadataEvent event) {
    addEventToBuffer(event);
    if (allEventsReceived()) {
      // start to commit the instant.
      boolean committed = commitInstant(this.instant);
      if (committed) {
        // The executor thread inherits the classloader of the #handleEventFromOperator
        // caller, which is a AppClassLoader.
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        // sync Hive synchronously if it is enabled in batch mode.
        syncHive();
        // schedules the compaction or clustering if it is enabled in batch execution mode
        scheduleTableServices(true);
      }
    }
  }

  private void scheduleTableServices(Boolean committed) {
    // if compaction is on, schedule the compaction
    if (tableState.scheduleCompaction) {
      CompactionUtil.scheduleCompaction(metaClient, writeClient, tableState.isDeltaTimeCompaction, committed);
    }
    // if clustering is on, schedule the clustering
    if (tableState.scheduleClustering) {
      ClusteringUtil.scheduleClustering(conf, writeClient, committed);
    }
  }

  private void handleWriteMetaEvent(WriteMetadataEvent event) {
    // the write task does not block after checkpointing(and before it receives a checkpoint success event),
    // if it checkpoints succeed then flushes the data buffer again before this coordinator receives a checkpoint
    // success event, the data buffer would flush with an older instant time.
    ValidationUtils.checkState(
        HoodieTimeline.compareTimestamps(this.instant, HoodieTimeline.GREATER_THAN_OR_EQUALS, event.getInstantTime()),
        String.format("Receive an unexpected event for instant %s from task %d",
            event.getInstantTime(), event.getTaskID()));

    addEventToBuffer(event);
  }

  /**
   * The coordinator reuses the instant if there is no data for this round of checkpoint,
   * sends the commit ack events to unblock the flushing.
   */
  private void sendCommitAckEvents(long checkpointId) {
    CompletableFuture<?>[] futures = Arrays.stream(this.gateways).filter(Objects::nonNull)
        .map(gw -> gw.sendEvent(CommitAckEvent.getInstance(checkpointId)))
        .toArray(CompletableFuture<?>[]::new);
    CompletableFuture.allOf(futures).whenComplete((resp, error) -> {
      if (!sendToFinishedTasks(error)) {
        throw new HoodieException("Error while waiting for the commit ack events to finish sending", error);
      }
    });
  }

  /**
   * Decides whether the given exception is caused by sending events to FINISHED tasks.
   *
   * <p>Ugly impl: the exception may change in the future.
   */
  private static boolean sendToFinishedTasks(Throwable throwable) {
    return throwable.getCause() instanceof TaskNotRunningException
        || throwable.getCause().getMessage().contains("running");
  }

  /**
   * Commits the instant.
   */
  private boolean commitInstant(String instant) {
    return commitInstant(instant, -1);
  }

  /**
   * Commits the instant.
   *
   * @return true if the write statuses are committed successfully.
   */
  private boolean commitInstant(String instant, long checkpointId) {
    if (Arrays.stream(eventBuffer).allMatch(Objects::isNull)) {
      // The last checkpoint finished successfully.
      return false;
    }

    List<WriteStatus> writeResults = Arrays.stream(eventBuffer)
        .filter(Objects::nonNull)
        .map(WriteMetadataEvent::getWriteStatuses)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    if (writeResults.size() == 0 && !OptionsResolver.allowCommitOnEmptyBatch(conf)) {
      // No data has written, reset the buffer and returns early
      reset();
      // Send commit ack event to the write function to unblock the flushing
      // If this checkpoint has no inputs while the next checkpoint has inputs,
      // the 'isConfirming' flag should be switched with the ack event.
      if (checkpointId != -1) {
        sendCommitAckEvents(checkpointId);
      }
      return false;
    }
    doCommit(instant, writeResults);
    return true;
  }

  /**
   * Performs the actual commit action.
   */
  @SuppressWarnings("unchecked")
  private void doCommit(String instant, List<WriteStatus> writeResults) {
    // commit or rollback
    long totalErrorRecords = writeResults.stream().map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);
    long totalRecords = writeResults.stream().map(WriteStatus::getTotalRecords).reduce(Long::sum).orElse(0L);
    boolean hasErrors = totalErrorRecords > 0;

    if (!hasErrors || this.conf.getBoolean(FlinkOptions.IGNORE_FAILED)) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      if (hasErrors) {
        LOG.warn("Some records failed to merge but forcing commit since commitOnErrors set to true. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }

      final Map<String, List<String>> partitionToReplacedFileIds = tableState.isOverwrite
          ? writeClient.getPartitionToReplacedFileIds(tableState.operationType, writeResults)
          : Collections.emptyMap();
      boolean success = writeClient.commit(instant, writeResults, Option.of(checkpointCommitMetadata),
          tableState.commitAction, partitionToReplacedFileIds);
      if (success) {
        reset();
        this.ckpMetadata.commitInstant(instant);
        LOG.info("Commit instant [{}] success!", instant);
      } else {
        throw new HoodieException(String.format("Commit instant [%s] failed!", instant));
      }
    } else {
      LOG.error("Error when writing. Errors/Total=" + totalErrorRecords + "/" + totalRecords);
      LOG.error("The first 100 error messages");
      writeResults.stream().filter(WriteStatus::hasErrors).limit(100).forEach(ws -> {
        LOG.error("Global error for partition path {} and fileID {}: {}",
            ws.getGlobalError(), ws.getPartitionPath(), ws.getFileId());
        if (ws.getErrors().size() > 0) {
          ws.getErrors().forEach((key, value) -> LOG.trace("Error for key:" + key + " and value " + value));
        }
      });
      // Rolls back instant
      writeClient.rollback(instant);
      throw new HoodieException(String.format("Commit instant [%s] failed and rolled back !", instant));
    }
  }

  @VisibleForTesting
  public WriteMetadataEvent[] getEventBuffer() {
    return eventBuffer;
  }

  @VisibleForTesting
  public String getInstant() {
    return instant;
  }

  @VisibleForTesting
  public Context getContext() {
    return context;
  }

  @VisibleForTesting
  public HoodieFlinkWriteClient getWriteClient() {
    return writeClient;
  }

  @VisibleForTesting
  public void setExecutor(NonThrownExecutor executor) throws Exception {
    if (this.executor != null) {
      this.executor.close();
    }
    this.executor = executor;
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Provider for {@link StreamWriteOperatorCoordinator}.
   */
  public static class Provider implements OperatorCoordinator.Provider {
    private final OperatorID operatorId;
    private final Configuration conf;

    public Provider(OperatorID operatorId, Configuration conf) {
      this.operatorId = operatorId;
      this.conf = conf;
    }

    @Override
    public OperatorID getOperatorId() {
      return this.operatorId;
    }

    @Override
    public OperatorCoordinator create(Context context) {
      return new StreamWriteOperatorCoordinator(this.conf, context);
    }
  }

  /**
   * Remember some table state variables.
   */
  private static class TableState implements Serializable {
    private static final long serialVersionUID = 1L;

    final WriteOperationType operationType;
    final String commitAction;
    final boolean isOverwrite;
    final boolean scheduleCompaction;
    final boolean scheduleClustering;
    final boolean syncHive;
    final boolean syncMetadata;
    final boolean isDeltaTimeCompaction;

    private TableState(Configuration conf) {
      this.operationType = WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION));
      this.commitAction = CommitUtils.getCommitActionType(this.operationType,
          HoodieTableType.valueOf(conf.getString(FlinkOptions.TABLE_TYPE).toUpperCase(Locale.ROOT)));
      this.isOverwrite = WriteOperationType.isOverwrite(this.operationType);
      this.scheduleCompaction = OptionsResolver.needsScheduleCompaction(conf);
      this.scheduleClustering = OptionsResolver.needsScheduleClustering(conf);
      this.syncHive = conf.getBoolean(FlinkOptions.HIVE_SYNC_ENABLED);
      this.syncMetadata = conf.getBoolean(FlinkOptions.METADATA_ENABLED);
      this.isDeltaTimeCompaction = OptionsResolver.isDeltaTimeCompaction(conf);
    }

    public static TableState create(Configuration conf) {
      return new TableState(conf);
    }
  }
}
