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

package com.splicemachine.derby.stream.utils;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

/**
 *
 * Utils for logging a locatedRow
 *
 */
public class StreamLogUtils {
    private static Logger LOG = Logger.getLogger(StreamLogUtils.class);

    public static void logOperationRecord(LocatedRow locatedRow, OperationContext operationContext) {
        if (LOG.isTraceEnabled()) {
            SpliceOperation op = operationContext.getOperation();
            SpliceLogUtils.trace(LOG, "%s (%d) -> %s", op.getName(),op.resultSetNumber(), locatedRow);
        }
    }

    public static void logOperationRecord(LocatedRow locatedRow, SpliceOperation operation) {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "%s (%d) -> %s", operation.getName(),operation.resultSetNumber(), locatedRow);
        }
    }


    public static void logOperationRecordWithMessage(LocatedRow locatedRow, OperationContext operationContext, String message) {
        if (LOG.isTraceEnabled()) {
            SpliceOperation op = operationContext.getOperation();
            SpliceLogUtils.trace(LOG, "%s (%d) [%s] -> %s", op.getName(),op.resultSetNumber(), message, locatedRow);
        }
    }

}
