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

package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.stats.ByteColumnStatistics;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.collector.ByteColumnStatsCollector;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class TinyintStatsCollector extends DvdStatsCollector{
    private final ByteColumnStatsCollector baseCollector;

    public TinyintStatsCollector(ByteColumnStatsCollector collector) {
        super(collector);
        this.baseCollector= collector;
    }

    @Override
    protected void doUpdate(DataValueDescriptor dataValueDescriptor, long count) throws StandardException {
        baseCollector.update(dataValueDescriptor.getByte(),count);
    }

    @Override
    protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
        return new TinyintStats((ByteColumnStatistics)build);
    }

}
