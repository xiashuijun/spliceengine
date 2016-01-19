package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.filter.HRowAccumulator;
import com.splicemachine.si.impl.filter.PackedTxnFilter;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.DataGet;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/13/14
 */
public class SITransactionReadController implements TransactionReadController{
    private final TxnSupplier txnSupplier;
    private final IgnoreTxnCacheSupplier ignoreTxnSuppler;

    public SITransactionReadController(TxnSupplier txnSupplier,
                                       IgnoreTxnCacheSupplier ignoreTxnSuppler){
        this.txnSupplier = txnSupplier;
        this.ignoreTxnSuppler = ignoreTxnSuppler;
    }

    @Override
    public void preProcessGet(DataGet get) throws IOException{
        get.returnAllVersions();
        get.setTimeRange(0,Long.MAX_VALUE);
    }

    @Override
    public void preProcessScan(DataScan scan) throws IOException{
        scan.setTimeRange(0l,Long.MAX_VALUE);
        scan.returnAllVersions();
    }

    @Override
    public TxnFilter newFilterState(ReadResolver readResolver,TxnView txn) throws IOException{
        return new SimpleTxnFilter(null,txn,readResolver,txnSupplier,ignoreTxnSuppler);
    }

    @Override
    public TxnFilter newFilterStatePacked(ReadResolver readResolver,
                                          EntryPredicateFilter predicateFilter,TxnView txn,boolean countStar) throws IOException{
        return new PackedTxnFilter(newFilterState(readResolver,txn),
                new HRowAccumulator(predicateFilter,new EntryDecoder(),countStar));
    }

    @Override
    public DDLFilter newDDLFilter(TxnView txn) throws IOException{
        return new DDLFilter(txn);
    }


}
