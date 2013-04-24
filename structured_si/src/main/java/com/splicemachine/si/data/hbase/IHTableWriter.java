package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.List;

public interface IHTableWriter {
    void write(HTableInterface table, Put put) throws IOException;
    void write(HRegion table, Put put) throws IOException;
    void write(HRegion region, Put put, Integer lock) throws IOException;
    void write(Object table, Put put, boolean durable) throws IOException;
    void write(HTableInterface table, List puts);

    RowLock lockRow(HTableInterface table, byte[] rowKey) throws IOException;
    Integer lockRow(HRegion region, byte[] rowKey) throws IOException;
    void unLockRow(HTableInterface table, RowLock lock) throws IOException;
    void unLockRow(HRegion region, Integer lock);
}
