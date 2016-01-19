package com.splicemachine.si;

import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.impl.MOpStatusFactory;
import com.splicemachine.si.impl.MOperationFactory;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
import com.splicemachine.si.impl.data.MExceptionFactory;
import com.splicemachine.si.testenv.SITestDataEnv;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.MFilterFactory;

/**
 * @author Scott Fines
 *         Date: 12/21/15
 */
public class MemSIDataEnv implements SITestDataEnv{
    private final MOperationFactory opFactory;
    private TxnOperationFactory txnOpFactory;

    public MemSIDataEnv(){
        this.opFactory=new MOperationFactory(new IncrementingClock());
        this.txnOpFactory = new SimpleTxnOperationFactory(MExceptionFactory.INSTANCE, opFactory);
    }

    @Override
    public DataFilterFactory getFilterFactory(){
        return MFilterFactory.INSTANCE;
    }

    @Override
    public ExceptionFactory getExceptionFactory(){
        return MExceptionFactory.INSTANCE;
    }

    @Override
    public OperationStatusFactory getOperationStatusFactory(){
        return MOpStatusFactory.INSTANCE;
    }

    @Override
    public TxnOperationFactory getOperationFactory(){
        return txnOpFactory;
    }

    @Override
    public OperationFactory getBaseOperationFactory(){
        return opFactory;
    }
}
