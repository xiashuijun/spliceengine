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

package com.splicemachine.derby.impl.sql.execute.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.splicemachine.derby.test.framework.*;
import org.sparkproject.guava.collect.Lists;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.test_dao.TableDAO;

/**
 * Test to validate that GENERATED columns work correctly.
 *
 * @author Scott Fines
 * Created on: 5/28/13
 */
public class GeneratedColumnIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final Logger LOG = Logger.getLogger(GeneratedColumnIT.class);
    private static final String CLASS_NAME = GeneratedColumnIT.class.getSimpleName().toUpperCase();

    protected  static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static SpliceTableWatcher generatedAlwaysTable = new SpliceTableWatcher("A", schemaWatcher.schemaName,"(adr_id integer NOT NULL GENERATED ALWAYS AS IDENTITY, adr_catid integer)");
    private static SpliceTableWatcher generatedDefaultTable = new SpliceTableWatcher("B", schemaWatcher.schemaName,"(adr_id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY, adr_catid integer)");
    private static SpliceTableWatcher generatedAlwaysTableStartsWith10 = new SpliceTableWatcher("C", schemaWatcher.schemaName,"(adr_id integer NOT NULL GENERATED ALWAYS AS IDENTITY( START WITH 10), adr_catid integer)");
    private static SpliceTableWatcher generatedAlwaysTableIncBy10 = new SpliceTableWatcher("D", schemaWatcher.schemaName,"(adr_id integer NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 10), adr_catid integer)");

    private static int size = 10;
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(generatedAlwaysTable)
            .around(generatedAlwaysTableStartsWith10)
            .around(generatedAlwaysTableIncBy10)
            .around(generatedDefaultTable)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format("insert into %s (adr_catid) values ?", generatedAlwaysTable.toString()));
                        for(int i=0;i<size;i++){
                            ps.setInt(1,i*10);
                            ps.execute();
                        }
                        ps = spliceClassWatcher.prepareStatement(String.format("insert into %s (adr_catid) values ?", generatedAlwaysTableStartsWith10));
                        for(int i=0;i<size;i++){
                            ps.setInt(1,i*10);
                            ps.execute();
                        }

												ps = spliceClassWatcher.prepareStatement(String.format("insert into %s (adr_id,adr_catid) values (DEFAULT,?)",generatedAlwaysTableStartsWith10));
												ps.setInt(1,10*size);
												ps.execute(); //make sure that we can add from default

                        ps = spliceClassWatcher.prepareStatement(String.format("insert into %s (adr_catid) values ?", generatedAlwaysTableIncBy10));
                        for(int i=0;i<size;i++){
                            ps.setInt(1,i*10);
                            ps.execute();
                        }

                        ps = spliceClassWatcher.prepareStatement(String.format("insert into %s (adr_catid) values ?",generatedDefaultTable));
                        for(int i=0;i<size/2;i++){
                            ps.setInt(1,i*10);
                            ps.execute();
                        }

                        ps = spliceClassWatcher.prepareStatement(String.format("insert into %s (adr_id,adr_catid) values (?,?)",generatedDefaultTable));
                        for(int i=size/2;i<size;i++){
                            ps.setInt(1,2*i);
                            ps.setInt(2,i*10);
                            ps.execute();
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }
            });


    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    private TableDAO tableDAO;

    @Before
    public void setUp() throws Exception {
        tableDAO = new TableDAO(methodWatcher.getOrCreateConnection());
    }

    @Test
    public void testCanInsertDefaultGeneratedData() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s", generatedDefaultTable));
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            Integer adrId = rs.getInt(1);

            Assert.assertTrue("No adr_id specified!",!rs.wasNull());
//            Assert.assertTrue("adrId falls in incorrect range! adrId = "+ adrId,size <= adrId || size/2 >= adrId);
            int addrCatId = rs.getInt(2);
            results.add(String.format("addrId=%d,addrCatId=%d",adrId,addrCatId));
        }
        for(String result:results){
            LOG.warn(result);
        }
        assertEquals("Incorrect number of rows returned!", size, results.size());
    }


    @Test
    public void testCanInsertGeneratedDataStartingWithValue() throws Exception {
       /*
        * Regression test for Bug 315. Make sure that the insertion which occurred during initialization is correct
        */
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s", generatedAlwaysTableStartsWith10));
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            Integer adrId = rs.getInt(1);

            Assert.assertTrue("No adr_id specified!",!rs.wasNull());
            Assert.assertTrue("adr_id outside correct range!adrId = "+ adrId,10<=adrId);
            int addrCatId = rs.getInt(2);
            results.add(String.format("addrId=%d,addrCatId=%d",adrId,addrCatId));
        }
        for(String result:results){
            LOG.debug(result);
        }
        assertEquals("Incorrect number of rows returned!", size + 1, results.size());
    }

    @Test
    public void testCanInsertGeneratedDataWithIncrement() throws Exception {
       /*
        * Regression test for Bug 315. Make sure that the insertion which occurred during initialization is correct
        */
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s", generatedAlwaysTableIncBy10));
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            Integer adrId = rs.getInt(1);

            Assert.assertTrue("No adr_id specified!",!rs.wasNull());
            Assert.assertTrue("(adrId-1)%10!=0, adrId="+adrId,(adrId-1)%10==0);
            int addrCatId = rs.getInt(2);
            results.add(String.format("addrId=%d,addrCatId=%d",adrId,addrCatId));
        }
        for(String result:results){
            LOG.debug(result);
        }
        assertEquals("Incorrect number of rows returned!", size, results.size());
    }

    @Test
    public void testCanInsertGeneratedData() throws Exception {
       /*
        * Regression test for Bug 315. Make sure that the insertion which occurred during initialization is correct
        */
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s", generatedAlwaysTable));
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            Integer adrId = rs.getInt(1);

            Assert.assertTrue("No adr_id specified!",!rs.wasNull());
            int addrCatId = rs.getInt(2);
            results.add(String.format("addrId=%d,addrCatId=%d",adrId,addrCatId));
        }
        for(String result:results){
            LOG.debug(result);
        }
        assertEquals("Incorrect number of rows returned!", size, results.size());
    }

    @Test
    public void testInsertGeneratedColumn() throws Exception {
        // DB-3656: generated column does not get updated for insert
        String tableName = "words".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        methodWatcher.execute(String.format("CREATE TABLE %s(WORD VARCHAR(20), UWORD GENERATED ALWAYS AS (UPPER(WORD)))",
                                            tableRef));
        methodWatcher.execute(String.format("INSERT INTO %s(WORD) VALUES 'chocolate', 'Coca-Cola', 'hamburger', " +
                                                "'carrot'",
                                            tableRef));

        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM %s", tableRef));
        while (rs.next()) {
            assertNotNull(rs.getString(2));
            assertEquals(rs.getString(1).toUpperCase(), rs.getString(2));
        }

    }

    @Test
    public void testUpdateGeneratedColumn() throws Exception {
        // DB-3656: generated column does not get updated for update
        String tableName = "arithmetic".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        methodWatcher.execute(String.format("CREATE TABLE %s(COL1 INT, COL2 INT, COL3 GENERATED ALWAYS AS (COL1+COL2))",
                                            tableRef));
        methodWatcher.execute(String.format("INSERT INTO %s (COL1, COL2) VALUES (1,2), (3,4), (5,6)",
                                            tableRef));

        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM %s", tableRef));
        while (rs.next()) {
            int col3expected = rs.getInt(1) + rs.getInt(2);
            assertEquals(col3expected, rs.getInt(3));
        }

        methodWatcher.execute(String.format("UPDATE %s SET COL2 = 100 WHERE COL1 = 1", tableRef));

        rs = methodWatcher.executeQuery(String.format("SELECT * FROM %s", tableRef));
        while (rs.next()) {
            int col3expected = rs.getInt(1) + rs.getInt(2);
            assertEquals(col3expected, rs.getInt(3));
        }

        rs = methodWatcher.executeQuery(String.format("select col3 from %s where col2 = 100",tableRef));
        assertTrue(rs.next());
        assertNotNull(rs.getInt(1));
        assertEquals(101, rs.getInt(1));
    }

    @Test
    public void testInsertGeneratedColumnIndex() throws Exception {
        // DB-3656: generated column does not get updated for insert
        String tableName = "words".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        methodWatcher.execute(String.format("CREATE TABLE %s(WORD VARCHAR(20), UWORD VARCHAR(20) GENERATED ALWAYS AS (UPPER(WORD)))",
                                            tableRef));
        methodWatcher.execute(String.format("INSERT INTO %s(WORD) VALUES 'chocolate', 'Coca-Cola', 'hamburger', " +
                                                "'carrot'",
                                            tableRef));

        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM %s", tableRef));
        while (rs.next()) {
            assertNotNull(rs.getString(2));
            assertEquals(rs.getString(1).toUpperCase(), rs.getString(2));
        }
        SpliceIndexWatcher.createIndex(methodWatcher.getOrCreateConnection(), schemaWatcher.schemaName, tableName,
                                       "uword_idx", "(uword)", true);

        rs = methodWatcher.executeQuery(String.format("select * from %s --SPLICE-PROPERTIES index = uword_idx \n" +
                                                          "where uword = 'CARROT'", tableRef));
        int n = 0;
        while (rs.next()) {
            assertNotNull(rs.getString(2));
            assertEquals(rs.getString(1).toUpperCase(), rs.getString(2));
            assertEquals("CARROT", rs.getString(2));
            n++;
        }
        assertEquals(1, n);
    }

    @Test
    public void testInsertGeneratedColumnUniqueConstraint() throws Exception {
        // DB-3656: generated column does not get updated for insert
        String tableName = "words".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        methodWatcher.execute(String.format("CREATE TABLE %s(WORD VARCHAR(20), UWORD VARCHAR(20) unique not null GENERATED ALWAYS AS (UPPER(WORD)))",
                                            tableRef));
        methodWatcher.execute(String.format("INSERT INTO %s(WORD) VALUES 'chocolate', 'Coca-Cola', 'hamburger', 'carrot'",
                                            tableRef));

        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM %s", tableRef));
        while (rs.next()) {
            assertNotNull(rs.getString(2));
            assertEquals(rs.getString(1).toUpperCase(), rs.getString(2));
        }

        try {
            methodWatcher.execute(String.format("INSERT INTO %s(WORD) VALUES 'Chocolate'", tableRef));
            fail("Expected unique constraint violation");
        } catch (SQLException e) {
            // expected
            assertEquals("23505", e.getSQLState());
        }
    }

    @Test
    public void testInsertGeneratedColumnPrimaryKey() throws Exception {
        // DB-3656: generated column does not get updated for insert
        String tableName = "words".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        methodWatcher.execute(String.format("CREATE TABLE %s(WORD VARCHAR(20), UWORD VARCHAR(20) primary key GENERATED ALWAYS AS (UPPER(WORD)))",
                                            tableRef));
        methodWatcher.execute(String.format("INSERT INTO %s(WORD) VALUES 'chocolate', 'Coca-Cola', 'hamburger', 'carrot'",
                                            tableRef));

        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM %s", tableRef));
        while (rs.next()) {
            assertNotNull(rs.getString(2));
            assertEquals(rs.getString(1).toUpperCase(), rs.getString(2));
        }

        try {
            methodWatcher.execute(String.format("INSERT INTO %s(WORD) VALUES 'Chocolate'", tableRef));
            fail("Expected unique constraint violation");
        } catch (SQLException e) {
            // expected
            assertEquals("23505", e.getSQLState());
        }
    }

    @Test
    public void testInsertGeneratedUniqueData() throws Exception {
        // DB-3665: generated identity column data jumps around
        // This test shows that all values generated are unique, (at least)

        // It also shows the nature of this bug when the SQL is specified as a 1-value increment when run on a cluster
        // On a cluster, each node gets its own batch (default 1000) of in-memory sequence IDs to hand out. When more
        // than one cluster node updates a table with one of these sequences each node has a different batch of IDs to
        // hand out. Thus, These sequences may increase by the batch size of the node.
        String tableName = "t1".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        methodWatcher.execute(String.format("create table %s(c1 int generated always as identity(start with 1, increment by 1), c2 int)",
                                            tableRef));
        for (int i=0; i<15; i++) {
            methodWatcher.execute(String.format("insert into %s(c2) values (8),(9)", tableRef));
            methodWatcher.execute(String.format("insert into %s(c2) select c1 from %s", tableRef,tableRef));
        }

        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM %s", tableRef));
        Set<Integer> uniques = new HashSet<>();
        int i=0;
        while (rs.next()) {
            i++;
            if (uniques.contains(rs.getInt(1))) {
                fail("Duplicate identity values at "+i+": "+rs.getInt(1));
            }
            uniques.add(rs.getInt(1));
        }
        assertEquals("Expected "+i+" unique values but got "+ uniques.size(), i, uniques.size());
    }

    @Test
    public void testInsertGenerateUniqueSequencedData() throws Exception {
        // DB-3665: generated identity column data jumps around
        // This test shows that, although all sequence values are unique, they are not in sequence order (1,2,...,n).
        // TODO: This test will fail when running on a cluster

        // The bug occurs when the SQL is specified as a 1-value increment when run on a cluster
        // On a cluster, each node gets its own batch (default 1000) of in-memory sequence IDs to hand out. When more
        // than one cluster node updates a table with one of these sequences each node has a different batch of IDs to
        // hand out. Thus, these sequences may increase by the batch size of the node which may produce gaps in the
        // sequence.
        String tableName = "t2".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        try(Statement s = conn.createStatement()){
            s.execute(String.format("create table %s(c1 int generated always as identity(start with 1,increment by 1), c2 int)", tableRef));
            int rowCount=0;
            for(int i=0;i<13;i++){
                rowCount+=s.executeUpdate(String.format("insert into %s(c2) values (8),(9)",tableRef));
                rowCount+=s.executeUpdate(String.format("insert into %s(c2) select c1 from %s",tableRef,tableRef));
            }

            try(ResultSet rs=s.executeQuery(String.format("SELECT * FROM %s order by c1",tableRef))){
                IntOpenHashSet foundData=new IntOpenHashSet(15);
                int lastValue=0;
                int count=0;
                while(rs.next()){
                    count++;
                    int next=rs.getInt(1);
                    Assert.assertFalse("Returned null!",rs.wasNull()); //ensure sequence is never null
                    Assert.assertTrue("Returned data < start value!",next>=1); //ensure sequence is always 1 or greater(start with 1)
                    Assert.assertTrue("Returned data out of order!",next>lastValue); //ensure sequence is sorted (ORDER BY clause)
                    Assert.assertTrue("Duplicate value ["+next+"] found in a unique sequence!",foundData.add(next)); //ensure sequence is unique(generated clause)
                    lastValue=next;
                }
                Assert.assertEquals("Incorrect returned row count!",rowCount,count);
            }
        }finally{
            try{
                conn.rollback();
                conn.reset();
            }catch(Exception e){
                Assert.fail("Unable to rollback:"+e.getMessage());
            }
        }
    }

    @Test
    public void repeatedInsertUniqueSequence() throws Exception{
        for(int i=0;i<10;i++){
            testInsertGenerateUniqueSequencedData();
        }
    }
}
