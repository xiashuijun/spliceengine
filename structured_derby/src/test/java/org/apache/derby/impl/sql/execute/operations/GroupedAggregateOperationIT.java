package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.util.*;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;

public class GroupedAggregateOperationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = GroupedAggregateOperationIT.class.getSimpleName().toUpperCase();
    public static final SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    public static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("OMS_LOG",CLASS_NAME,"(swh_date date, i integer)");
    public static final SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("T8",CLASS_NAME,"(c1 int, c2 int)");

    @ClassRule
    public static TestRule rule = RuleChain.outerRule(spliceSchemaWatcher)
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/employee-2table.sql", CLASS_NAME))
            .around(spliceTableWatcher)
            .around(spliceTableWatcher2)            
            		.around(new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {
                try {                	
                    spliceClassWatcher.setAutoCommit(true);    
                    spliceClassWatcher.executeUpdate(format("insert into %s values (date('2012-01-01'),1)", spliceTableWatcher));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (date('2012-02-01'),1)", spliceTableWatcher));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (date('2012-03-01'),1)", spliceTableWatcher));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (date('2012-03-01'),2)", spliceTableWatcher));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (date('2012-03-01'),3)", spliceTableWatcher));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (date('2012-04-01'),3)", spliceTableWatcher));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (date('2012-05-01'),3)", spliceTableWatcher));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (null, null), (1,1), (null, null), (2,1), (3,1),(10,10)", spliceTableWatcher2));
                    
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    spliceClassWatcher.closeAll();
                }
            }

        });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    // Confirm baseline HAVING support
    public void testHaving() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
              format("SELECT T1.PNUM FROM %s.T1 T1 GROUP BY T1.PNUM " + 
                 "HAVING T1.PNUM IN ('P1', 'P2', 'P3')",
                  CLASS_NAME));
        Assert.assertEquals(3, TestUtils.resultSetToMaps(rs).size());
    }

    @Test
    // Bugzilla #376: nested sub-query in HAVING
    public void testHavingWithSubQuery() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("SELECT T1.PNUM FROM %1$s.T1 T1 GROUP BY T1.PNUM " +
                "HAVING T1.PNUM IN " +
                // Progressively more simple sub-queries:
                "(SELECT T2.PNUM FROM %1$s.T2 T2 GROUP BY T2.PNUM HAVING SUM(T2.BUDGET) > 25000)",
                //"(SELECT T2.PNUM FROM %s.T2 T2 WHERE T2.PNUM IN ('P1', 'P2', 'P3'))",
                //"(SELECT 'P1' FROM %s.T2)",
                CLASS_NAME, CLASS_NAME));
        Assert.assertEquals(3, TestUtils.resultSetToMaps(rs).size());
    }

    @Test
    // Bugzilla #377: nested sub-query in HAVING with ORDER BY
    public void testHavingWithSubQueryAndOrderby() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("SELECT T1.PNUM FROM %1$s.T1 T1 GROUP BY T1.PNUM " +
                "HAVING T1.PNUM IN  (SELECT T2.PNUM FROM %1$s.T2 T2 GROUP BY T2.PNUM HAVING SUM(T2.BUDGET) > 25000)" +
                "ORDER BY T1.PNUM",
                CLASS_NAME));
        List<Map> maps = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(3, maps.size());
        Assert.assertEquals("P2", maps.get(0).get("PNUM"));
    }

    @Test()
    // Bugzilla #581: WORKDAY: count(distinct()) fails in a GROUP BY clause
    public void countDistinctInAGroupByClause() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select distinct month(swh_date), count(distinct(i)) from %s " +
                "group by month(swh_date) order by month(swh_date)",
                spliceTableWatcher.toString()));
        int i = 0;
        while (rs.next()) {
        	i++;
        	if (rs.getInt(1) == 3 && rs.getInt(2) != 3) {
        		Assert.assertTrue("count distinct did not return 3 for month 3",false);
        	}
        }
        Assert.assertEquals("Should return only rows for the group by columns",5, i);	
    }
    
    @Test
    public void testCountOfNullsAndBooleanSet() throws Exception {
         /*
          * According to sqlFiddle (e.g. MySQL and Postgres), the correct
          * output of this query is:
          *
          * if the table looks like
          *
          * null | null
          * 1    | 1
          * null | null
          * 2    | 1
          * 3    | 1
          * 10   | 10
          *
          * then the output should be
          *
          * T | 1
          * T | 1
          * T | 1
          * T | 1
          * T | 0
          *
          * (although not necessarily in that order). While that looks totally weird-ass, it's considered
          * correct, so that's what we will test for here
          */
        ResultSet rs = methodWatcher.executeQuery(format("select (1 in (1,2)), count(c1) from %s group by c1",spliceTableWatcher2));
        List<Pair<Boolean,Integer>> results = Lists.newArrayListWithExpectedSize(5);
        while (rs.next()) {
            boolean truth = rs.getBoolean(1);
            int count = rs.getInt(2);
            results.add(Pair.newPair(truth,count));
        }
        Collections.sort(results,new Comparator<Pair<Boolean, Integer>>() {
            @Override
            public int compare(Pair<Boolean, Integer> o1, Pair<Boolean, Integer> o2) {
                if(o1==null){
                    if(o2==null) return 0;
                    return -1;
                }else if(o2==null){
                    return 1;
                }

                if(o1.getFirst()){
                    if(!o2.getFirst()) return -1; //put true before false
                    //they are both true, compare second field
                    return o1.getSecond().compareTo(o2.getSecond());
                }else if(o2.getFirst()) return 1;

                //they are both false
                return o1.getSecond().compareTo(o2.getSecond());
            }
        });
        @SuppressWarnings("unchecked") List<Pair<Boolean,Integer>> correctResults = Arrays.asList(
                (Pair<Boolean,Integer>[])new Pair[]{
                        Pair.newPair(true,0),
                        Pair.newPair(true,1),
                        Pair.newPair(true,1),
                        Pair.newPair(true,1),
                        Pair.newPair(true,1)
                }
        );


        Assert.assertArrayEquals("Should return only rows for the group by columns",correctResults.toArray(),results.toArray());
    }    

    @Test()
    // Bugzilla #787
    public void testNoExplicitAggregationFunctions() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select c1+10, c2, c1*1, c1, c2*5 from %s group by c1, c2",spliceTableWatcher2));
        int i =0;
        while (rs.next()) {
        	i++;
        }
        Assert.assertEquals("Should return 5 rows",5, i);	
    }    

    @Test()
    // Bugzilla #790
    public void testAggregateWithSingleReturnedValue() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select tmpC1 from (select max(c1+10) from %s group by c2) as tmp (tmpC1)",spliceTableWatcher2));
        int i =0;
        while (rs.next()) {
        	i++;
        }
        Assert.assertEquals("Should return 3 rows",3, i);	
    } 
        
}
