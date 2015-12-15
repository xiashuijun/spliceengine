package com.splicemachine.si.data.hbase;

import com.splicemachine.si.api.data.SExceptionLib;
import com.splicemachine.si.api.data.SOperationStatusLib;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import java.io.IOException;

/**
 * Created by jleach on 12/14/15.
 */
public class HOperationStatusLib implements SOperationStatusLib<OperationStatus> {
   private static final SExceptionLib exceptionLib = SIDriver.getExceptionLib();
    public static final OperationStatus NOT_RUN = new OperationStatus(HConstants.OperationStatusCode.NOT_RUN);
    public static final OperationStatus SUCCESS = new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
    public static final OperationStatus FAILURE = new OperationStatus(HConstants.OperationStatusCode.FAILURE);


    @Override
    public OperationStatus getNotRun() {
        return NOT_RUN;
    }

    @Override
    public OperationStatus getSuccess() {
        return SUCCESS;
    }

    @Override
    public OperationStatus getFailure() {
        return FAILURE;
    }

    @Override
    public boolean isSuccess(OperationStatus operationStatus) {
        return operationStatus.getOperationStatusCode() == HConstants.OperationStatusCode.SUCCESS;
    }

    @Override
    public OperationStatus[] getArrayOfSize(int size) {
        return new OperationStatus[size];
    }

    @Override
    public boolean processPutStatus(OperationStatus operationStatus) throws IOException {
        switch (operationStatus.getOperationStatusCode()) {
            case NOT_RUN:
                throw new IOException("Could not acquire Lock");
            case BAD_FAMILY:
                throw exceptionLib.getNoSuchColumnFamilyException(operationStatus.getExceptionMsg());
            case SANITY_CHECK_FAILURE:
                throw new IOException("Sanity Check failure:" + operationStatus.getExceptionMsg());
            case FAILURE:
                throw new IOException(operationStatus.getExceptionMsg());
            default:
                return true;
        }
    }

    @Override
    public OperationStatus getCorrectStatus(OperationStatus status, OperationStatus oldStatus) {
        switch (oldStatus.getOperationStatusCode()) {
            case SUCCESS:
                return status;
            case NOT_RUN:
            case BAD_FAMILY:
            case SANITY_CHECK_FAILURE:
            case FAILURE:
                return oldStatus;
        }
        return null;
    }

    @Override
    public OperationStatus getAdditiveWriteConflictOperationStatus() {
        return new OperationStatus(HConstants.OperationStatusCode.FAILURE, exceptionLib.getAdditiveWriteConflict().getMessage());
    }

    @Override
    public OperationStatus newFailureOperationStatus(Exception ex) {
        return new OperationStatus(HConstants.OperationStatusCode.FAILURE, ex.getMessage());
    }
}
