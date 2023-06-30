/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;

import com.wepay.kafka.connect.bigquery.write.row.GCSToBQWriter;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A Runnable that runs a GCS to BQ Load task.
 *
 * <p>This task goes through the given GCS bucket, and takes as many blobs as a single load job per
 * table can handle (as defined here: https://cloud.google.com/bigquery/quotas#load_jobs) and runs
 * those load jobs. Blobs are deleted (only) once a load job involving that blob succeeds.
 */
public class GCSToBQLoadRunnable implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(GCSToBQLoadRunnable.class);

  private final BigQuery bigQuery;
  private final Storage storage;
  private final String bucket;
  private final String directoryPrefix;
  private final Set<TableId> targetTableIds;
  private final Map<Job, List<BlobId>> activeJobs;
  private final Set<BlobId> claimedBlobIds;
  private final Set<BlobId> deletableBlobIds;

  // these numbers are intended to try to make this task not excede Google Cloud Quotas.
  // see: https://cloud.google.com/bigquery/quotas#load_jobs

  // max number of files we can load in a single load job
  private static int FILE_LOAD_LIMIT = 10000;
  // max total size (in bytes) of the files we can load in a single load job.
  private static long MAX_LOAD_SIZE_B = 15 * 1000000000000L; // 15TB

  private static String SOURCE_URI_FORMAT = "gs://%s/%s";
  public static final Pattern METADATA_TABLE_PATTERN =
          Pattern.compile("((?<project>[^:]+):)?(?<dataset>[^.]+)\\.(?<table>.+)");

  private static String podName = System.getenv("CONNECT_POD_NAME");
  /**
   * Create a {@link GCSToBQLoadRunnable} with the given bigquery, bucket, and ms wait interval.
   * @param bigQuery the {@link BigQuery} instance.
   * @param storage the {@link Storage} instance.
   * @param bucket the the GCS bucket to read from.
   * @param topicsToBaseTableIds
   */
  public GCSToBQLoadRunnable(BigQuery bigQuery, Storage storage, String bucket, String directoryPrefix, Map<String, TableId> topicsToBaseTableIds) {
    this.bigQuery = bigQuery;
    this.storage = storage;
    this.bucket = bucket;
    this.directoryPrefix = directoryPrefix;
    this.targetTableIds = topicsToBaseTableIds.values().stream().collect(Collectors.toSet());
    this.activeJobs = new HashMap<>();
    this.claimedBlobIds = new HashSet<>();
    this.deletableBlobIds = new HashSet<>();
  }

  /**
   * Return a map of {@link TableId}s to a list of {@link Blob}s intended to be batch-loaded into
   * that table.
   *
   * <p>Each blob list will not exceed the {@link #FILE_LOAD_LIMIT} in number of blobs or
   * {@link #MAX_LOAD_SIZE_B} in total byte size. Blobs that are already claimed by an in-progress
   * load job will also not be included.
   * @return map from {@link TableId}s to {@link Blob}s.
   */
  private Map<TableId, List<Blob>> getBlobsUpToLimit() {
    Map<TableId, List<Blob>> tableToURIs = new HashMap<>();
    Map<TableId, Long> tableToCurrentLoadSize = new HashMap<>();

    logger.trace("Starting GCS bucket list");
    Page<Blob> list = storage.list(
            bucket,
            Storage.BlobListOption.prefix(directoryPrefix)
    );
//    Page<Blob> list = bucket.list(Storage.BlobListOption.prefix(directoryPrefix));
    logger.trace("Finished GCS bucket list");
    logger.debug("getBlobsUpToLimit bucket: {}", bucket);
    logger.debug("Storage.BlobListOption.prefix(directoryPrefix) : {}", Storage.BlobListOption.prefix(directoryPrefix));

    String blobName = null;

    for (Blob blob : list.iterateAll()) {
      logger.debug("bucket blob: {}", blob);
      logger.debug("blob.getBlobId(): {}", blob.getBlobId());

      BlobId blobId = blob.getBlobId();
      TableId table = getTableFromBlob(blob);


      logger.debug("Checking blob bucket={}, name={}, table={} ", blob.getBucket(), blob.getName(), table);
      logger.debug("claimedBlobIds: {}", claimedBlobIds);
      logger.debug("deletableBlobIds: {}", deletableBlobIds);
      logger.debug("targetTableIds: {}", targetTableIds);

      if (table == null
              || claimedBlobIds.contains(blobId)
              || deletableBlobIds.contains(blobId)
              || !targetTableIds.contains(table)) {        // don't do anything if:
        // 1. we don't know what table this should be uploaded to or
        // 2. this blob is already claimed by a currently-running job or
        // 3. this blob is up for deletion.
        // 4. this blob is not targeted for our target  tables
        logger.debug("Inside if block targetTableIds: {}", targetTableIds);
        logger.debug("Inside if block table: {}", table);
        continue;
      }

      if (!tableToURIs.containsKey(table)) {
        // initialize maps, if we haven't seen this table before.
        tableToURIs.put(table, new ArrayList<>());
        tableToCurrentLoadSize.put(table, 0L);
        logger.debug("Inside tableToURIs: {}", tableToURIs);
        logger.debug("Inside tableToURIs.get(table): {}", tableToURIs.get(table));
      }
      logger.debug("tableToURIs: {}", tableToURIs);
      logger.debug("tableToURIs.get(table): {}", tableToURIs.get(table));
      logger.debug("tableToURIs.get(table).size(): {}", tableToURIs.get(table).size());
      logger.debug("tableToCurrentLoadSize: {}", tableToCurrentLoadSize);
      logger.debug("tableToCurrentLoadSize.get(table): {}", tableToCurrentLoadSize.get(table));
      logger.debug("blob.getSize(): {}", blob.getSize());

      long newSize = tableToCurrentLoadSize.get(table) + blob.getSize();
      logger.debug("newSize: {}", newSize);
      // if this file does not cause us to exceed our per-request quota limits...
      if (newSize < MAX_LOAD_SIZE_B && tableToURIs.get(table).size() < FILE_LOAD_LIMIT) {
        logger.debug("Inside newsize check");
        // ...add the file (and update the load size)
        tableToURIs.get(table).add(blob);
        tableToCurrentLoadSize.put(table, newSize);

        logger.debug("Inside newsize tableToURIs: {}", tableToURIs);
        logger.debug("Inside newsize tableToCurrentLoadSize: {}", tableToCurrentLoadSize);
      }
    }

    logger.debug("Got blobs to upload: {}", tableToURIs);
    return tableToURIs;
  }

  /**
   * Given a blob, return the {@link TableId} this blob should be inserted into.
   * @param blob the blob
   * @return the TableId this data should be loaded into, or null if we could not tell what
   *         table it should be loaded into.
   */
  public static TableId getTableFromBlob(Blob blob) {

    //

    logger.debug("Inside getTableFromBlob");
    logger.debug("blob.getMetadata(): {}",blob.getMetadata());
    logger.debug("blob.getBucket(): {}",blob.getBucket());
    logger.debug("blob.getName(): {}",blob.getName());
    logger.debug("GCSToBQWriter.GCS_METADATA_TABLE_KEY: {}",blob.getMetadata().get(GCSToBQWriter.GCS_METADATA_TABLE_KEY));

    if (blob.getMetadata() == null
        || blob.getMetadata().get(GCSToBQWriter.GCS_METADATA_TABLE_KEY) == null) {
      logger.error("Found blob {}/{} with no metadata.", blob.getBucket(), blob.getName());
      return null;
    }

    String serializedTableId = blob.getMetadata().get(GCSToBQWriter.GCS_METADATA_TABLE_KEY);
    Matcher matcher = METADATA_TABLE_PATTERN.matcher(serializedTableId);

    if (!matcher.find()) {
      logger.error("Found blob `{}/{}` with un-parsable table metadata.",
          blob.getBucket(), blob.getName());
      return null;
    }

    String project = matcher.group("project");
    String dataset = matcher.group("dataset");
    String table =  matcher.group("table");
    //table = FieldNameSanitizer.sanitizeName(table);
    logger.debug("Table data: project: {}; dataset: {}; table: {}", project, dataset, table);
    logger.info("Table data: project: {}; dataset: {}; table: {}", project, dataset, table);
    if (project == null) {
      return TableId.of(dataset, table);
    } else {
      return TableId.of(project, dataset, table);
    }
  }

  /**
   * Trigger a BigQuery load job for each table in the input containing all the blobs associated
   * with that table.
   * @param tablesToBlobs a map of {@link TableId} to the list of {@link Blob}s to be loaded into
   *                      that table.
   * @return a map from Jobs to the list of blobs being loaded in that job.
   */
  private Map<Job, List<Blob>> triggerBigQueryLoadJobs(Map<TableId, List<Blob>> tablesToBlobs) {
    Map<Job, List<Blob>> newJobs = new HashMap<>(tablesToBlobs.size());
    logger.debug("tablesToBlobs: {}", tablesToBlobs);
    for (Map.Entry<TableId, List<Blob>> entry : tablesToBlobs.entrySet()) {
      logger.debug("entry.getKey(): {}", entry.getKey());
      logger.debug("entry.getValue(): {}", entry.getValue());
      newJobs.put(triggerBigQueryLoadJob(entry.getKey(), entry.getValue()), entry.getValue());
    }
    return newJobs;
  }

  private Job triggerBigQueryLoadJob(TableId table, List<Blob> blobs) {
    List<String> uris = blobs.stream()
                             .map(b -> String.format(SOURCE_URI_FORMAT,
                                                     bucket,
                                                     b.getName()))
                             .collect(Collectors.toList());
    // create job load configuration
    logger.debug("uris: {}",uris);
    LoadJobConfiguration loadJobConfiguration =
        LoadJobConfiguration.newBuilder(table, uris)
            .setFormatOptions(FormatOptions.json())
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();
    // create and return the job.
    Job job = bigQuery.create(JobInfo.of(loadJobConfiguration));
    // update active jobs and claimed blobs.
    List<BlobId> blobIds = blobs.stream().map(Blob::getBlobId).collect(Collectors.toList());
    logger.debug("inside triggerBigQueryLoadJob blobIds: {}",blobIds);
    activeJobs.put(job, blobIds);
    claimedBlobIds.addAll(blobIds);
    logger.debug("inside triggerBigQueryLoadJob activeJobs: {}",activeJobs);
    logger.debug("inside triggerBigQueryLoadJob claimedBlobIds: {}",claimedBlobIds);
    logger.info("Triggered load job for table {} with {} blobs.", table, blobs.size());
    return job;
  }

  /**
   * Check all active jobs. Remove those that have completed successfully and log a message for
   * any jobs that failed. We only log a message for failed jobs because those blobs will be
   * retried during the next run.
   */
  private void checkJobs() {
    if (activeJobs.isEmpty()) {
      // quick exit if nothing needs to be done.
      logger.debug("No active jobs to check. Skipping check jobs.");
      return;
    }
    logger.debug("Checking {} active jobs", activeJobs.size());

    Iterator<Map.Entry<Job, List<BlobId>>> jobIterator = activeJobs.entrySet().iterator();
    int successCount = 0;
    int failureCount = 0;

    while (jobIterator.hasNext()) {
      Map.Entry<Job, List<BlobId>> jobEntry = jobIterator.next();
      Job job = jobEntry.getKey();
      logger.debug("Checking next job: {}", job.getJobId());

      try {
        //waiting load job until finished
        job = job.waitFor();
        if (job.isDone()) {
          // log a message job's rows count
          JobStatistics.LoadStatistics stats = job.getStatistics();
          logger.trace("Job is row count: id={}, count={}", job.getJobId(), stats.getOutputRows());
          logger.trace("Job is marked done: id={}, status={}", job.getJobId(), job.getStatus());
          logger.debug("Job is marked done: id={}, status={}", job.getJobId(), job.getStatus());
          List<BlobId> blobIdsToDelete = jobEntry.getValue();
          jobIterator.remove();
          logger.trace("Job is removed from iterator: {}", job.getJobId());
          successCount++;
          claimedBlobIds.removeAll(blobIdsToDelete);
          logger.trace("Completed blobs have been removed from claimed set: {}", blobIdsToDelete);
          deletableBlobIds.addAll(blobIdsToDelete);
          logger.debug(" active jobs to delete : {}",deletableBlobIds);
          logger.trace("Completed blobs marked as deletable: {}", blobIdsToDelete);
        }
      } catch (BigQueryException | InterruptedException ex) {
        // log a message.
        logger.warn("GCS to BQ load job failed", ex);
        // remove job from active jobs (it's not active anymore)
        List<BlobId> blobIds = activeJobs.get(job);
        jobIterator.remove();
        // unclaim blobs
        claimedBlobIds.removeAll(blobIds);
        failureCount++;
      } finally {
        logger.info("GCS To BQ job tally: {} successful jobs, {} failed jobs.",
                    successCount, failureCount);
      }
    }
  }

  private List<BlobId> archiveBlobs(List<BlobId> blobIdsToDelete) {
    List<BlobId> resultList = new ArrayList<>();
    for (BlobId blobId: blobIdsToDelete){
      logger.debug("Inside archiveBlobs {}",blobId);
      if(!moveBlob(blobId)) {
        resultList.add(blobId);
      }
    }
    return resultList;
  }

  private boolean moveBlob(BlobId blobId){
    String bucketName = bucket;
    Blob blob = storage.get(blobId);
    String blobName = blob.getName();
    logger.debug("Inside GCStoBQ blobName {}",blobName);
    //String directory = blobName.substring(0, blobName.indexOf('/'));
    String directory = blobName.substring(0, blobName.lastIndexOf('/'));
    logger.debug("Inside GCStoBQ directory {}",directory);
    logger.debug("Inside GCStoBQ directory 111{}",blobName.substring(0, blobName.lastIndexOf('/')));
    String jsonName = blobName.substring(blobName.lastIndexOf('/') + 1);
    logger.debug("Inside GCStoBQ jsonName {}",jsonName);
    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    String targetName = String.format("archive/%s/dt=%s/%s",
            directory,
            date,
            jsonName
    );
    logger.debug("Inside GCStoBQ targetName {}",targetName);
    return blob.copyTo(bucketName, targetName).isDone();
  }

  /**
   * Delete deletable blobs.
   */
  private void deleteBlobs() {
    List<BlobId> blobIdsToDelete = new ArrayList<>();
    blobIdsToDelete.addAll(deletableBlobIds);
    int numberOfBlobs = blobIdsToDelete.size();
    int failedDeletes = 0;
    int successfulDeletes = 0;

    if (numberOfBlobs == 0) {
      logger.info("No blobs to delete");
      return;
    }

    logger.info("Attempting to delete {} blobs", numberOfBlobs);

    try {
      // Archive the blobs
      List<BlobId> archiveResults = archiveBlobs(blobIdsToDelete);
      int archiveFailCount = archiveResults.size();
      if(archiveFailCount == 0){
        logger.info("Successfully archived all blobs");
      } else {
        for(BlobId blobId: archiveResults){
          logger.info("Failed to archive {}", blobId.getName());
        }
      }
      // Issue a batch delete api call
      List<Boolean> resultList = storage.delete(blobIdsToDelete);

      // Filter the blobs we couldn't delete from the list of deletable blobs
      for (int i = 0; i < numberOfBlobs; i++) {
        if (!resultList.get(i)) {
          // This blob was not successful, remove it from the list.
          // Adjust the target index by the number of failed deletes we've
          // already seen since we're mutating the list as we go.
          int targetIndex = i - failedDeletes;
          blobIdsToDelete.remove(targetIndex);
          failedDeletes++;
        }
      }

      // Calculate number of successful deletes, remove the successful deletes from
      // the deletableBlobIds.
      successfulDeletes = numberOfBlobs - failedDeletes;
      deletableBlobIds.removeAll(blobIdsToDelete);

      logger.info("Successfully deleted {} blobs; failed to delete {} blobs",
                  successfulDeletes,
                  failedDeletes);
    } catch (StorageException ex) {
      logger.warn("Storage exception while attempting to delete blobs", ex);
    } catch (Exception e){
      logger.error("Failed to delete because of {}", e.getMessage());
      throw e;
    }
  }

  @Override
  public void run() {
    logger.trace("Starting BQ load run");
    try {
      logger.debug("Checking for finished job statuses. Moving uploaded blobs from claimed to deletable.");
      checkJobs();
     /* logger.trace("Deleting deletable blobs");
      deleteBlobs();*/
      logger.debug("Finding new blobs to load into BQ");
      Map<TableId, List<Blob>> tablesToSourceURIs = getBlobsUpToLimit();
      logger.debug("Loading {} new blobs into BQ", tablesToSourceURIs.size());
      triggerBigQueryLoadJobs(tablesToSourceURIs);
      logger.debug("Finished BQ load run");
      logger.debug("Deleting deletable blobs");
      deleteBlobs();

    } catch (Exception e) {
      logger.error("Uncaught error in BQ loader", e);
    }
  }
}
