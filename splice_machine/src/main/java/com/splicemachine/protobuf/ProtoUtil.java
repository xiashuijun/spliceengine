package com.splicemachine.protobuf;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.derby.DerbyMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Created by jleach on 11/13/15.
 */
public class ProtoUtil {
    private static final Logger LOG = Logger.getLogger(ProtoUtil.class);
    private static Function TABLEDESCRIPTORTOUUID = new Function<TableDescriptor,DerbyMessage.UUID>() {
        @Override
        public DerbyMessage.UUID apply(@Nullable TableDescriptor td) {
            return transferDerbyUUID((BasicUUID)td.getUUID());
        }
    };

    public static DDLChange alterStats(long txnId, List<TableDescriptor> tableDescriptors) {
        return DDLChange.newBuilder().setTxnId(txnId)
                .setAlterStats(AlterStats.newBuilder().addAllTableId(Lists.transform(tableDescriptors,TABLEDESCRIPTORTOUUID)))
                .setDdlChangeType(DDLChangeType.ALTER_STATS)
                .build();
    }

    public static DDLChange createDropSchema(long txnId, String schemaName) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropSchema(DropSchema.newBuilder()
                .setSchemaName(schemaName).build())
                .setDdlChangeType(DDLChangeType.DROP_SCHEMA)
                .build();
    }


    public static DDLChange createTrigger(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setCreateTrigger(CreateTrigger.newBuilder()
                .setTableId(transferDerbyUUID(basicUUID)))
                .setDdlChangeType(DDLChangeType.CREATE_TRIGGER)
                .build();
    }

    public static DDLChange dropTrigger(long txnId, BasicUUID tableUUID, BasicUUID triggerUUID, BasicUUID spsDescriptorUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropTrigger(DropTrigger.newBuilder()
                .setTableId(transferDerbyUUID(tableUUID))
                .setTriggerId(transferDerbyUUID(triggerUUID))
                .setSpsDescriptorUUID(transferDerbyUUID(spsDescriptorUUID)))
                .setDdlChangeType(DDLChangeType.DROP_TRIGGER)
                .build();
    }

    public static DerbyMessage.UUID transferDerbyUUID(BasicUUID basicUUID) {
        return DerbyMessage.UUID.newBuilder().setMajorId(basicUUID.majorId)
                .setSequence(basicUUID.sequence)
                .setTimemillis(basicUUID.timemillis).build();
    }

    public static BasicUUID getDerbyUUID(DerbyMessage.UUID messageUUID) {
        return new BasicUUID(messageUUID.getMajorId(),messageUUID.getTimemillis(),messageUUID.getSequence());
    }


    public static DDLChange createDropTable(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropTable(DropTable.newBuilder()
                .setTableId(transferDerbyUUID(basicUUID)))
                .setDdlChangeType(DDLChangeType.DROP_TABLE)
                .build();
    }

    public static DDLChange createTable(long txnId) {
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.CREATE_TABLE)
                .build();
    }

    public static DDLChange createSchema(long txnId) {
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.CREATE_SCHEMA)
                .build();
    }

    public static DDLChange createAlias(long txnId) {
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.CREATE_ALIAS)
                .build();
    }

    public static DDLChange dropAlias(long txnId, String schemaName, String aliasName, String namespace) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropAlias((DropAlias.newBuilder()
                .setSchemaName(schemaName)
                .setAliasName(aliasName)
                .setNamespace(namespace)))
                .setDdlChangeType(DDLChangeType.DROP_ALIAS)
                .build();
    }

    public static DDLChange createAlterTable(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setAlterTable(AlterTable.newBuilder()
                .addTableId(transferDerbyUUID(basicUUID)))
                .setDdlChangeType(DDLChangeType.ALTER_TABLE)
                .build();
    }

    public static DDLChange createRenameTable(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setRenameTable(RenameTable.newBuilder()
                .setTableId(transferDerbyUUID(basicUUID)))
                .setDdlChangeType(DDLChangeType.RENAME_TABLE)
                .build();
    }

    public static DDLChange createRenameIndex(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setRenameIndex(RenameIndex.newBuilder()
                .setTableId(transferDerbyUUID(basicUUID)))
                .setDdlChangeType(DDLChangeType.RENAME_INDEX)
                .build();
    }

    public static DDLChange createRenameColumn(long txnId, BasicUUID basicUUID, String oldColumnName) {
        return DDLChange.newBuilder().setTxnId(txnId).setRenameColumn(RenameColumn.newBuilder()
                .setTableId(transferDerbyUUID(basicUUID))
                .setColumnName(oldColumnName))
                .setDdlChangeType(DDLChangeType.RENAME_COLUMN)
                .build();
    }


    public static FKConstraintInfo createFKConstraintInfo(ForeignKeyConstraintDescriptor fKConstraintDescriptor) {
        ColumnDescriptorList columnDescriptors = fKConstraintDescriptor.getColumnDescriptors();
        return FKConstraintInfo.newBuilder().setTableName(fKConstraintDescriptor.getTableDescriptor().getName())
                .setConstraintName(fKConstraintDescriptor.getConstraintName())
                .setColumnNames(Joiner.on(",").join(Lists.transform(columnDescriptors, new ColumnDescriptorNameFunction()))).build();
    }

    private static class ColumnDescriptorNameFunction implements Function<ColumnDescriptor, String> {
        @Override
        public String apply(ColumnDescriptor columnDescriptor) {
            return columnDescriptor.getColumnName();
        }
    }

    public static DDLChange createDropIndex(long indexConglomId, long tableConglomId, long txnId, BasicUUID tableUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropIndex(DropIndex.newBuilder()
                .setBaseConglomerate(tableConglomId)
                .setTableUUID(transferDerbyUUID(tableUUID))
                .setConglomerate(indexConglomId))
                .setDdlChangeType(DDLChangeType.DROP_INDEX)
                .build();
    }

    public static Index createIndex(long conglomerate, IndexDescriptor indexDescriptor) {
        boolean[] ascColumns = indexDescriptor.isAscending();
        Index.Builder builder=Index.newBuilder()
                .setConglomerate(conglomerate)
                .setUniqueWithDuplicateNulls(indexDescriptor.isUniqueWithDuplicateNulls())
                .setUnique(indexDescriptor.isUnique());
        for(int i=0;i<ascColumns.length;i++){
            builder = builder.addDescColumns(!ascColumns[i]);
        }

        int[] backingArray=indexDescriptor.baseColumnPositions();
        for(int i=0;i<backingArray.length;i++){
            builder = builder.addIndexColsToMainColMap(backingArray[i]);
        }
        return builder.build();
    }

    public static Table createTable(long conglomerate, TableDescriptor td, LanguageConnectionContext lcc) throws StandardException {
        assert td!=null:"TableDescriptor is null";
        assert td.getFormatIds()!=null:"No Format ids";
        SpliceConglomerate sc = (SpliceConglomerate)((SpliceTransactionManager)lcc.getTransactionExecute()).findConglomerate(conglomerate);
        return Table.newBuilder()
                .setConglomerate(conglomerate)
                .addAllFormatIds(Ints.asList(td.getFormatIds()))
                .addAllColumnOrdering(Ints.asList(sc.getColumnOrdering()))
                .setTableVersion(DataDictionaryUtils.getTableVersion(lcc,td.getUUID()))
                .setTableUuid(transferDerbyUUID((BasicUUID)td.getUUID())).build();
    }

    public static DDLChange createTentativeIndexChange(long txnId, LanguageConnectionContext lcc, long baseConglomerate, long indexConglomerate,
                                                       TableDescriptor td, IndexDescriptor indexDescriptor) throws StandardException {
        SpliceLogUtils.trace(LOG, "create Tentative Index {baseConglomerate=%d, indexConglomerate=%d");
        return DDLChange.newBuilder().setTentativeIndex(TentativeIndex.newBuilder()
                .setIndex(createIndex(indexConglomerate, indexDescriptor))
                .setTable(createTable(baseConglomerate,td,lcc))
                .build())
                .setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.CREATE_INDEX)
                .build();
    }

    public static TentativeIndex createTentativeIndex(LanguageConnectionContext lcc, long baseConglomerate, long indexConglomerate,
                                                       TableDescriptor td, IndexDescriptor indexDescriptor) throws StandardException {
        SpliceLogUtils.trace(LOG, "create Tentative Index {baseConglomerate=%d, indexConglomerate=%d");
        return TentativeIndex.newBuilder()
                .setIndex(createIndex(indexConglomerate,indexDescriptor))
                .setTable(createTable(baseConglomerate,td,lcc))
                .build();
    }

    public static DDLChange createRestoreMode(long txnId) {
        return DDLChange.newBuilder().setTxnId(txnId).setDdlChangeType(DDLChangeType.ENTER_RESTORE_MODE).build();
    }

    public static DDLChange createDropPKConstraint(long txnId, long newConglomId, long oldConglomId,
                                                   int[] srcColumnOrdering, int[] targetColumnOrdering,
                                                   ColumnInfo[] columInfos, LanguageConnectionContext lcc, BasicUUID tableId) throws StandardException {
        String tableVersion = DataDictionaryUtils.getTableVersion(lcc, tableId);
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.DROP_PRIMARY_KEY)
                .setTentativeDropPKConstraint(
                        TentativeDropPKConstraint.newBuilder()
                                .setNewConglomId(newConglomId)
                                .setOldConglomId(oldConglomId)
                                .setTableVersion(tableVersion)
                                .addAllSrcColumnOrdering(Ints.asList(srcColumnOrdering))
                                .addAllTargetColumnOrdering(Ints.asList(targetColumnOrdering))
                                .setColumnInfos(ZeroCopyLiteralByteString.wrap(DDLUtils.serializeColumnInfoArray(columInfos))
                                )).build();
    }

    public static DDLChange createTentativeAddColumn(long txnId, long newCongNum,
                                                     long oldCongNum, int[] columnOrdering,
                                                     ColumnInfo[] newColumnInfo, LanguageConnectionContext lcc, BasicUUID tableId) throws StandardException {
        String tableVersion = DataDictionaryUtils.getTableVersion(lcc, tableId);
        return DDLChange.newBuilder().setTxnId(txnId).setDdlChangeType(DDLChangeType.ADD_COLUMN)
                .setTentativeAddColumn(TentativeAddColumn.newBuilder()
                                .setTableVersion(tableVersion)
                                .setOldConglomId(oldCongNum)
                                .setNewConglomId(newCongNum)
                                .addAllColumnOrdering(columnOrdering!=null?Ints.asList(columnOrdering):Collections.EMPTY_LIST)
                                .setColumnInfo(ZeroCopyLiteralByteString.wrap(DDLUtils.serializeColumnInfoArray(newColumnInfo)))
                ).build();
    }

    public static DDLChange createTentativeDropColumn(long txnId, long newCongNum,
                                                     long oldCongNum, int[] oldColumnOrdering, int[] newColumnOrdering,
                                                     ColumnInfo[] columnInfo, int droppedColumnPosition, LanguageConnectionContext lcc, BasicUUID tableId) throws StandardException {
        String tableVersion = DataDictionaryUtils.getTableVersion(lcc, tableId);
        return DDLChange.newBuilder().setTxnId(txnId).setDdlChangeType(DDLChangeType.DROP_COLUMN)
                .setTentativeDropColumn(TentativeDropColumn.newBuilder()
                                .setTableVersion(tableVersion)
                                .setOldConglomId(oldCongNum)
                                .setNewConglomId(newCongNum)
                                .addAllOldColumnOrdering(Ints.asList(oldColumnOrdering))
                                .addAllNewColumnOrdering(Ints.asList(newColumnOrdering))
                                .setColumnInfos(ZeroCopyLiteralByteString.wrap(DDLUtils.serializeColumnInfoArray(columnInfo)))
                                .setDroppedColumnPosition(droppedColumnPosition)
                ).build();
    }

    public static DDLChange createTentativeDropConstraint (long txnId, long oldConglomId, long indexConglomId, LanguageConnectionContext lcc, BasicUUID tableId) throws StandardException {
        String tableVersion = DataDictionaryUtils.getTableVersion(lcc, tableId);
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.DROP_CONSTRAINT)
                .setTentativeDropConstraint(TentativeDropConstraint.newBuilder()
                                .setTableVersion(tableVersion)
                                .setOldConglomId(oldConglomId)
                                .setIndexConglomerateId(indexConglomId)
                ).build();
    }

    public static DDLChange createTentativeAddConstraint (long txnId, long oldConglomId,
                                                          long newConglomId, long indexConglomerateId,
                                                          int[] srcColumnOrdering, int[] targetColumnOrdering, ColumnInfo[] columnInfo, LanguageConnectionContext lcc, BasicUUID tableId) throws StandardException {
        String tableVersion = DataDictionaryUtils.getTableVersion(lcc, tableId);
    return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.ADD_UNIQUE_CONSTRAINT)
                .setTentativeAddConstraint(TentativeAddConstraint.newBuilder()
                                .setNewConglomId(newConglomId)
                                .setOldConglomId(oldConglomId)
                                .setIndexConglomerateId(indexConglomerateId)
                                .setTableVersion(tableVersion)
                                .addAllSrcColumnOrdering(Ints.asList(srcColumnOrdering))
                                .addAllTargetColumnOrdering(Ints.asList(targetColumnOrdering))
                                .setColumnInfos(ZeroCopyLiteralByteString.wrap(DDLUtils.serializeColumnInfoArray(columnInfo)))
                ).build();


    }

    public static DDLChange createTentativeFKConstaint (ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor, long txnId,
                                                        long baseConglomerate, String tableName, String tableVersion,
                                                        int[] backingIndexFormatIds, long backingIndexConglomerateId) {
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.ADD_FOREIGN_KEY)
                .setTentativeFK(TentativeFK.newBuilder()
                                .addAllBackingIndexFormatIds(Ints.asList(backingIndexFormatIds))
                                .setBaseConglomerate(baseConglomerate)
                                .setReferencedTableName(tableName)
                                .setReferencedTableVersion(tableVersion)
                                .setFkConstraintInfo(createFKConstraintInfo(foreignKeyConstraintDescriptor))
                                .setBackingIndexConglomerateId(backingIndexConglomerateId)
                ).build();
    }

    public static DDLChange createNoOpDDLChange(long txnId, String changeId) {
        return DDLChange.newBuilder()
                .setTxnId(txnId)
                .setChangeId(changeId)
                .build();
    }

    public static DDLChange createDropIndexTrigger(long indexConglomId, long tableConglomId, long txnId) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropIndexTrigger(DropIndexTrigger.newBuilder()
                .setBaseConglomerate(tableConglomId)
                .setConglomerate(indexConglomId))
                .setDdlChangeType(DDLChangeType.DROP_INDEX_TRIGGER)
                .build();
    }

    public static DDLChange createDropRole(long txnId, String roleName) {
        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.DROP_ROLE)
                .setTxnId(txnId)
                .setDropRole(DropRole.newBuilder().setRoleName(roleName).build())
                .build();
    }

    public static DDLChange createTruncateTable(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.TRUNCATE_TABLE)
                .setTxnId(txnId)
                .setTruncateTable(TruncateTable.newBuilder().setTableId(transferDerbyUUID(basicUUID)).build())
                .build();
    }

    public static DDLChange createRevokeTablePrivilege(long txnId, TablePermsDescriptor permissionsDescriptor) {
        RevokeTablePrivilege revokeTablePrivilege = RevokeTablePrivilege.newBuilder()
                .setTableId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getTableUUID()))
                .setSelectPerm(permissionsDescriptor.getSelectPriv())
                .setDeletePerm(permissionsDescriptor.getDeletePriv())
                .setInsertPerm(permissionsDescriptor.getInsertPriv())
                .setUpdatePerm(permissionsDescriptor.getUpdatePriv())
                .setReferencesPerm(permissionsDescriptor.getReferencesPriv())
                .setTriggerPerm(permissionsDescriptor.getTriggerPriv())
                .setGrantor(permissionsDescriptor.getGrantor())
                .setGrantee(permissionsDescriptor.getGrantee())
                .setPermObjectId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getUUID()))
                .build();
        RevokePrivilege revokePrivilege = RevokePrivilege.newBuilder()
                .setType(RevokePrivilege.Type.REVOKE_TABLE_PRIVILEGE)
                .setRevokeTablePrivilege(revokeTablePrivilege)
                .build();

        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.REVOKE_PRIVILEGE)
                .setTxnId(txnId).setRevokePrivilege(revokePrivilege).build();
    }

    public static DDLChange createRevokeColumnPrivilege(long txnId, ColPermsDescriptor permissionsDescriptor) {

        RevokeColumnPrivilege revokeColumnPrivilege = RevokeColumnPrivilege.newBuilder()
                .setTableId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getTableUUID()))
                .setType(permissionsDescriptor.getType())
                .setGrantee(permissionsDescriptor.getGrantee())
                .setGrantor(permissionsDescriptor.getGrantor())
                .setColumns(ByteString.copyFrom(permissionsDescriptor.getColumns().getByteArray()))
                .setPermObjectId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getUUID()))
                .build();

        RevokePrivilege revokePrivilege = RevokePrivilege.newBuilder()
                .setType(RevokePrivilege.Type.REVOKE_COLUMN_PRIVILEGE)
                .setRevokeColumnPrivilege(revokeColumnPrivilege)
                .build();

        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.REVOKE_PRIVILEGE)
                .setTxnId(txnId).setRevokePrivilege(revokePrivilege).build();
    }

    public static DDLChange createRevokeRoutinePrivilege(long txnId, RoutinePermsDescriptor permissionsDescriptor) {

        RevokeRoutinePrivilege revokeRoutinePrivilege = RevokeRoutinePrivilege.newBuilder()
                .setRountineId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getRoutineUUID()))
                .setGrantee(permissionsDescriptor.getGrantee())
                .setGrantor(permissionsDescriptor.getGrantor())
                .setPermObjectId(transferDerbyUUID((BasicUUID)permissionsDescriptor.getUUID()))
                .build();

        RevokePrivilege revokePrivilege = RevokePrivilege.newBuilder()
                .setType(RevokePrivilege.Type.REVOKE_ROUTINE_PRIVILEGE)
                .setRevokeRoutinePrivilege(revokeRoutinePrivilege)
                .build();

        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.REVOKE_PRIVILEGE)
                .setTxnId(txnId).setRevokePrivilege(revokePrivilege).build();
    }

    public static DDLChange createRevokeGenericPrivilege(long txnId, PermDescriptor permissionsDescriptor, boolean restrict) {
        RevokeGenericPrivilege revokeGenericPrivilege = RevokeGenericPrivilege.newBuilder()
                .setId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getPermObjectId()))
                .setObjectType(permissionsDescriptor.getObjectType())
                .setPermObjectId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getPermObjectId()))
                .setGrantor(permissionsDescriptor.getGrantor())
                .setGrantee(permissionsDescriptor.getGrantee())
                .setGrantable(permissionsDescriptor.isGrantable())
                .setRestrict(restrict)
                .build();

        RevokePrivilege revokePrivilege = RevokePrivilege.newBuilder()
                .setType(RevokePrivilege.Type.REVOKE_GENERIC_PRIVILEGE)
                .setRevokeGenericPrivilege(revokeGenericPrivilege)
                .build();
        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.REVOKE_PRIVILEGE)
                .setTxnId(txnId).setRevokePrivilege(revokePrivilege).build();
    }
}






