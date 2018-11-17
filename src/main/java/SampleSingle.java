/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.*;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


public class SampleSingle {

    private static final Logger log = LoggerFactory.getLogger(SampleSingle.class);

    private final String applicationName = "reader-java-old"; // The name of the metric

    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L; // Amount of time before S3 PUT actions are taken
    private static final long Idle_Millis_Between_Calls = 10L; // Time between GetRecords calls
    private static final int MAX_RECORDS_IN_BUFFER = 10000; // Maximum amount of records that can be buffered
    private static final int Producer_Period_MS = 1; // Time between Producer PUTS

    private static final int MAX_RECORDS_PER_CALL = 10000; // Amount of max Records that can be retrieved in one GET call

    public static void main(String... args) {
        if (args.length < 1) {
            log.error("At a minimum stream name is required as the first argument. Region may be specified as the second argument");
            System.exit(1);
        }

        String streamName = args[0];
        String region = null;
        if (args.length > 1) {
            region = args[1];
        }

        SampleSingle processor = new SampleSingle(streamName);
        processor.run();
    }

    private final String streamName;
    private final AmazonKinesisAsync kinesisClient;

    private SampleSingle(String streamName) {
        this.streamName = streamName;
        this.kinesisClient = AmazonKinesisAsyncClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
    }

    private void run() {
        ScheduledExecutorService producerExecutor = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> producerFuture = producerExecutor.scheduleAtFixedRate(this::publishRecord, 10, Producer_Period_MS, TimeUnit.MILLISECONDS);

        String workerId = applicationName + "_" + UUID.randomUUID();

        KinesisClientLibConfiguration kinesisClientLibConfiguration =
            new KinesisClientLibConfiguration(
                applicationName,
                streamName,
                DefaultAWSCredentialsProviderChain.getInstance(),
                workerId)
                    .withRegionName("us-west-2")
                    .withIdleMillisBetweenCalls(Idle_Millis_Between_Calls)
                    .withMaxRecords(MAX_RECORDS_PER_CALL)
                    .withMaxRecordsCount(MAX_RECORDS_IN_BUFFER)
                    .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        IRecordProcessorFactory recordProcessorFactory = new SampleRecordProcessorFactory();

        final Worker worker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(kinesisClientLibConfiguration)
                .build();

        Thread schedulerThread = new Thread(worker::run);
        schedulerThread.setDaemon(true);
        schedulerThread.start();

        System.out.println("Press enter to shutdown");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            reader.readLine();
        } catch (IOException ioex) {
            log.error("Caught exception while waiting for confirm.  Shutting down", ioex);
        }

        log.info("Cancelling producer, and shutting down excecutor.");
        producerFuture.cancel(true);
        producerExecutor.shutdownNow();

        Future<Boolean> gracefulShutdownFuture = worker.startGracefulShutdown();
        log.info("Waiting up to 20 seconds for shutdown to complete.");
        try {
            gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.info("Interrupted while waiting for graceful shutdown. Continuing.");
        } catch (ExecutionException e) {
            log.error("Exception while executing graceful shutdown.", e);
        } catch (TimeoutException e) {
            log.error("Timeout while waiting for shutdown.  Scheduler may not have exited.");
        }
        log.info("Completed, shutting down now.");
    }

    private Integer putCount = 0;

    private void publishRecord() {
        long createTime = System.currentTimeMillis();
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(streamName);
        putRecordRequest.setData(ByteBuffer.wrap(String.format("testData-%d", putCount++).getBytes()));
        putRecordRequest.setPartitionKey(String.format("partitionKey-%d", createTime));
        kinesisClient.putRecord(putRecordRequest);
    }

    /**
     * Used to create new record processors.
     */
    public class SampleRecordProcessorFactory  implements IRecordProcessorFactory {
        /**
         * {@inheritDoc}
         */
        @Override
        public IRecordProcessor createProcessor() {
            return new SampleRecordProcessor();
        }
    }

    private static class SampleRecordProcessor implements IRecordProcessor {

        private static final String SHARD_ID_MDC_KEY = "ShardId";

        private static final Logger log = LoggerFactory.getLogger(SampleRecordProcessor.class);

        private String shardId;

        private final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();

        private long nextCheckpointTimeInMillis;
        private final List<String> recordBuffer = new ArrayList<String>();
        private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

        /**
         * {@inheritDoc}
         */
        @Override
        public void initialize(InitializationInput input) {
            this.shardId = input.getShardId();
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            List<Record> records = processRecordsInput.getRecords();
            if (records != null) {
                try {
                    long millUntilDump = nextCheckpointTimeInMillis - System.currentTimeMillis();
                    log.info("Processing {} record(s). Dump time remaining: {}", records.size(), millUntilDump);
                    for (int i = 0; i < records.size(); i++) {
                        recordBuffer.add(decoder.decode(records.get(i).getData()).toString());
                    }
                    if (millUntilDump < 0) {
                        sendToS3AndCheckpoint(processRecordsInput.getCheckpointer());
                        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
                    }
                } catch (Throwable t) {
                    log.error("Caught throwable while processing records. Aborting");
                    log.info(t.toString());
                    log.info(t.getStackTrace().toString());
                    Thread.currentThread().interrupt();
                } finally {
                    MDC.remove(SHARD_ID_MDC_KEY);
                }
            }
        }

        private void sendToS3AndCheckpoint(IRecordProcessorCheckpointer checkpointer) throws InvalidStateException, ShutdownException {
            PutObjectResult res = s3Client.putObject("xealth.dev.analytics", "kcl_example/" + UUID.randomUUID().toString() + ".txt", String.join("\n", recordBuffer));
            log.info(res.toString());
            recordBuffer.clear();
            checkpointer.checkpoint();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(ShutdownInput shutdownInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            log.info("Shutting down record processor for shard");
            try {
                // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
                if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
                    sendToS3AndCheckpoint(shutdownInput.getCheckpointer());
                }
            } catch (ShutdownException | InvalidStateException e) {
                log.error("Exception while checkpointing at requested shutdown.  Giving up", e);
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }
    }
}