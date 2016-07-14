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

package com.splicemachine.derby.impl.sql.execute.operations.iapi;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;

import java.io.Externalizable;

/**
 * @author Scott Fines
 *         Created on: 11/4/13
 */
public interface AggregateContext extends Externalizable {

    void init(SpliceOperationContext context) throws StandardException;

    SpliceGenericAggregator[] getAggregators() throws StandardException;

    ExecIndexRow getSortTemplateRow() throws StandardException;

    ExecIndexRow getSourceIndexRow();

    SpliceGenericAggregator[] getDistinctAggregators();

    SpliceGenericAggregator[] getNonDistinctAggregators();
}
