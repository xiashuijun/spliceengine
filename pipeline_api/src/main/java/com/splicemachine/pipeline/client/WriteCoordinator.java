/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.pipeline.client;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.IOException;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.PipelineConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.callbuffer.BufferConfiguration;
import com.splicemachine.pipeline.callbuffer.PipingCallBuffer;
import com.splicemachine.pipeline.callbuffer.PreFlushHook;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.config.DefaultWriteConfiguration;
import com.splicemachine.pipeline.config.ForwardingWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.threadpool.MonitoredThreadPool;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.pipeline.writer.AsyncBucketingWriter;
import com.splicemachine.pipeline.writer.SynchronousBucketingWriter;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;

/**
 * Entry point for classes that want to write. Use this class to get CallBuffer<KVPair> for a given table.
 *
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class WriteCoordinator {

    private final Writer asynchronousWriter;
    private final Writer synchronousWriter;
    private final Monitor monitor;
    private final WriteConfiguration defaultWriteConfiguration;
    private final PartitionFactory partitionFactory;
    private final MonitoredThreadPool writerPool;

    public static WriteCoordinator create(SConfiguration config,
                                          BulkWriterFactory writerFactory,
                                          PipelineExceptionFactory exceptionFactory,
                                          PartitionFactory partitionFactory,
                                          Clock clock) throws IOException {
        assert config != null;
        MonitoredThreadPool writerPool = MonitoredThreadPool.create(config);
        int maxEntries = config.getMaxBufferEntries();//SpliceConstants.maxBufferEntries;
        Writer writer = new AsyncBucketingWriter(writerPool,
                writerFactory,
                exceptionFactory,
                partitionFactory,clock);
        Writer syncWriter = new SynchronousBucketingWriter(writerFactory,exceptionFactory,partitionFactory,clock);
        long maxBufferHeapSize = config.getMaxBufferHeapSize();
        int numRetries = config.getMaxRetries();
        long pause = config.getClientPause();
        int maxFlushesPerRegion = config.getWriteMaxFlushesPerRegion();
        Monitor monitor = new Monitor(maxBufferHeapSize, maxEntries, numRetries, pause, maxFlushesPerRegion);

        return new WriteCoordinator(writer, syncWriter, monitor,partitionFactory,exceptionFactory,writerPool);
    }

    public WriteCoordinator(Writer asynchronousWriter,
                             Writer synchronousWriter,
                             Monitor monitor,
                             PartitionFactory partitionFactory,
                             PipelineExceptionFactory pipelineExceptionFactory,
                            MonitoredThreadPool writerPool) {
        this.asynchronousWriter = asynchronousWriter;
        this.synchronousWriter = synchronousWriter;
        this.monitor = monitor;
        this.defaultWriteConfiguration = new DefaultWriteConfiguration(monitor,pipelineExceptionFactory);
        this.partitionFactory = partitionFactory;
        this.writerPool = writerPool;
    }

    /**
     * Used to register this coordinator with JMX
     */
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName coordinatorName = new ObjectName(PipelineConfiguration.WRITE_COORDINATOR_OBJECT_LOCATION);
        mbs.registerMBean(monitor, coordinatorName);
        asynchronousWriter.registerJMX(mbs);
        synchronousWriter.registerJMX(mbs);
    }

    public void start() { }

    public void shutdown() {
        asynchronousWriter.stopWrites();
    }

    public void setMaxAsyncThreads(int count){
        writerPool.setMaxThreadCount(count);
    }

    public int getMaxAsyncThreads(){
        return writerPool.getMaxThreadCount();
    }

    public WriteConfiguration defaultWriteConfiguration() {
        return defaultWriteConfiguration;
    }

    public RecordingCallBuffer<KVPair> writeBuffer(Partition partition, TxnView txn) {
        return writeBuffer(partition,txn, PipelineUtils.noOpFlushHook);
    }

    public RecordingCallBuffer<KVPair> synchronousWriteBuffer(Partition partition, TxnView txn) {
        return synchronousWriteBuffer(partition,txn, PipelineUtils.noOpFlushHook,defaultWriteConfiguration);
    }

    public RecordingCallBuffer<KVPair> writeBuffer(Partition partition, TxnView txn, PreFlushHook preFlushHook) {
        return writeBuffer(partition,txn,preFlushHook,Metrics.noOpMetricFactory());
    }

    public RecordingCallBuffer<KVPair> noIndexWriteBuffer(byte[] tableId, TxnView txn, final MetricFactory metricFactory) throws IOException{
        PartitionFactory pf = SIDriver.driver().getTableFactory();
        Partition p = pf.getTable(tableId);
        return noIndexWriteBuffer(p,txn,metricFactory);
    }

    public RecordingCallBuffer<KVPair> noIndexWriteBuffer(Partition partition, TxnView txn, final MetricFactory metricFactory) {
        WriteConfiguration config = defaultWriteConfiguration;
        //if it isn't active, don't bother creating the extra object
        if (metricFactory.isActive()) {
            config = new ForwardingWriteConfiguration(defaultWriteConfiguration) {
                @Override public MetricFactory getMetricFactory() { return metricFactory; }
            };
        }
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(partition, txn, asynchronousWriter, PipelineUtils.noOpFlushHook, config, monitor, true);
   }


    public RecordingCallBuffer<KVPair> writeBuffer(Partition partition, TxnView txn, final PreFlushHook preFlushHook,final MetricFactory metricFactory) {
        WriteConfiguration config = defaultWriteConfiguration;
        monitor.outstandingBuffers.incrementAndGet();
        //if it isn't active, don't bother creating the extra object
        if (metricFactory.isActive()) {
            config = new ForwardingWriteConfiguration(defaultWriteConfiguration) {
                @Override public MetricFactory getMetricFactory() { return metricFactory; }
            };
        }
        return writeBuffer(partition, txn, preFlushHook,config);
    }

    public RecordingCallBuffer<KVPair> writeBuffer(byte[] partition, TxnView txn, PreFlushHook preFlushHook) throws IOException{
        return writeBuffer(partition, txn, preFlushHook, defaultWriteConfiguration,Metrics.noOpMetricFactory());
    }

    public RecordingCallBuffer<KVPair> writeBuffer(byte[] partition, TxnView txn, MetricFactory metricsFactory) throws IOException{
        return writeBuffer(partition, txn, PipelineUtils.noOpFlushHook, defaultWriteConfiguration,metricsFactory);
    }

    public RecordingCallBuffer<KVPair> writeBuffer(byte[] partition,TxnView txn,PreFlushHook noOpFlushHook,
                                                   WriteConfiguration writeConfig,MetricFactory metricsFactory) throws IOException{
        Partition p =SIDriver.driver().getTableFactory().getTable(partition);
        return writeBuffer(p,txn,noOpFlushHook,writeConfig);
    }

    public RecordingCallBuffer<KVPair> writeBuffer(Partition partition, TxnView txn,
                                                   PreFlushHook flushHook, WriteConfiguration writeConfiguration) {
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(partition, txn, asynchronousWriter, flushHook, writeConfiguration, monitor, false);
    }

    public RecordingCallBuffer<KVPair> writeBuffer(Partition partition, TxnView txn, final int maxEntries) {
        BufferConfiguration config = new BufferConfiguration() {
            @Override public long getMaxHeapSize() { return Long.MAX_VALUE; }
            @Override public int getMaxEntries() { return maxEntries; }
            @Override public int getMaxFlushesPerRegion() { return monitor.getMaxFlushesPerRegion(); }
            @Override public void writeRejected() { monitor.writeRejected(); }
        };
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(partition, txn, asynchronousWriter, PipelineUtils.noOpFlushHook, defaultWriteConfiguration, config, false);
    }

    public RecordingCallBuffer<KVPair> synchronousWriteBuffer(Partition partition,
                                                              TxnView txn, PreFlushHook flushHook,
                                                              WriteConfiguration writeConfiguration) {
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(partition, txn, synchronousWriter, flushHook, writeConfiguration, monitor, false);
    }

    public RecordingCallBuffer<KVPair> synchronousWriteBuffer(Partition partition,
                                                              TxnView txn,
                                                              PreFlushHook flushHook,
                                                              WriteConfiguration writeConfiguration,
                                                              final int maxEntries) {
        BufferConfiguration config = new BufferConfiguration() {
            @Override public long getMaxHeapSize() { return Long.MAX_VALUE; }
            @Override public int getMaxEntries() { return maxEntries; }
            @Override public int getMaxFlushesPerRegion() { return monitor.getMaxFlushesPerRegion(); }
            @Override public void writeRejected() { monitor.writeRejected(); }
        };
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(partition, txn, synchronousWriter, flushHook, writeConfiguration, config, false);
    }

    public PartitionFactory getPartitionFactory(){ return partitionFactory; }

    public RecordingCallBuffer<KVPair> writeBuffer(Partition table,TxnView txn,WriteConfiguration writeConfiguration){
        return writeBuffer(table,txn,PipelineUtils.noOpFlushHook,writeConfiguration);
    }

    private class MonitoredPipingCallBuffer extends PipingCallBuffer {

        public MonitoredPipingCallBuffer(Partition partition,
                                         TxnView txn,
                                         Writer writer,
                                         PreFlushHook preFlushHook,
                                         WriteConfiguration writeConfiguration,
                                         BufferConfiguration bufferConfiguration,
                                         boolean skipIndexWrites) {
            super(partition, txn, writer, preFlushHook, writeConfiguration, bufferConfiguration, skipIndexWrites);
        }

        @Override
        public void close() throws Exception {
            monitor.outstandingBuffers.decrementAndGet();
            super.close();
        }
    }

}