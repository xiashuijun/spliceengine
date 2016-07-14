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

package com.splicemachine.stats.frequency;

import org.sparkproject.guava.collect.Lists;
import org.sparkproject.guava.primitives.Longs;
import com.splicemachine.hash.HashFunctions;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class LongSpaceSaverTest {

    @Test
    public void testFrequentElementsCorrectWithNoEviction() throws Exception {
        LongFrequencyCounter counter = new LongSpaceSaver(HashFunctions.murmur3(0),100);

        fillPowersOf2(counter);

        LongFrequentElements fe = counter.frequentElements(2);
        checkMultiplesOf8(fe);
        checkMultiplesOf4(counter.frequentElements(4));
    }

    @Test
    public void testFrequentElementsCorrectWithEviction() throws Exception {
        LongFrequencyCounter counter = new LongSpaceSaver(HashFunctions.murmur3(0),3);

        fillPowersOf2(counter);

        LongFrequentElements fe = counter.frequentElements(3);
        checkMultiplesOf8(fe);
//        checkMultiplesOf4(counter.frequentElements(8));
    }

    private void checkMultiplesOf8(LongFrequentElements fe) {
        Assert.assertEquals("Incorrect value for -8!",15,fe.equal(-8l).count());
        FrequencyEstimate<? extends Long> equal=fe.equal(0l);
        Assert.assertEquals("Incorrect value for 0!",15,equal.count()-equal.error());

        //check before -8
        assertEmpty("Values <-8 found!",fe.frequentBefore(-8l,false));
        //check includes -8
        List<TestFrequency> correct = Arrays.asList(new TestFrequency(-8, 15,0));
        assertMatches("Values <-8 found!",correct,fe.frequentBefore(-8l, true));

        //check from -8 to 0
        correct = Arrays.asList(new TestFrequency(-8,15,0),new TestFrequency(0,15,0));
        assertMatches("Incorrect values for range [-8,0]!",correct,fe.frequentBetween(-8l, 0l, true, true));
        correct = Arrays.asList(new TestFrequency(-8,15,0));
        assertMatches("Incorrect values for range [-8,0)!",correct,fe.frequentBetween(-8l, 0l, true, false));
        correct = Arrays.asList(new TestFrequency(0,15,0));
        assertMatches("Incorrect values for range (-8,0]!",correct,fe.frequentBetween(-8l, 0l, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (-8,0]!",correct,fe.frequentBetween(-8l, 0l, false, false));

        /*
         * This portion isn't a correct test, because it's possible for error to push elements >0 into
         * the set
         */
        //check from 0 to 8
//        assertMatches("Values >0 found!", Collections.<TestFrequency>emptyList(), fe.frequentAfter(0l, false));
//        correct = Arrays.asList(new TestFrequency(0,15,0));
//        assertMatches("Incorrect values for range [0,8]!",correct,fe.frequentBetween(0l, 8l, true, true));
//        assertMatches("Incorrect values for range [0,8)!",correct,fe.frequentBetween(0, 8, true, false));
//        correct = Collections.emptyList();
//        assertMatches("Incorrect values for range (0,8]!",correct,fe.frequentBetween(0, 8, false, true));
//        assertMatches("Incorrect values for range (0,8)!",correct,fe.frequentBetween(0, 8, false, false));
    }

    private void checkMultiplesOf4(LongFrequentElements fe) {
        Assert.assertEquals("Incorrect value for -8!",15,fe.equal(-8l).count());
        Assert.assertEquals("Incorrect value for -4!",7,fe.equal(-4l).count()-fe.equal(-4l).error());
        Assert.assertEquals("Incorrect value for 0!",15,fe.equal(0l).count());
        Assert.assertEquals("Incorrect value for -4!",7,fe.equal(4l).count()-fe.equal(4l).error());

        //check before -8
        assertEmpty("Values <-8 found!",fe.frequentBefore(-8l,false));
        //check includes -8
        List<TestFrequency> correct = Arrays.asList(new TestFrequency(-8, 15,0));
        assertMatches("Values <-8 found!",correct,fe.frequentBefore(-8, true));

        //check from -8 to -4
        correct = Arrays.asList(new TestFrequency(-8,15,0),new TestFrequency(-4,7,0));
        assertMatches("Incorrect values for range [-8,-4]!",correct,fe.frequentBetween(-8, -4, true, true));
        correct = Arrays.asList(new TestFrequency(-8,15,0));
        assertMatches("Incorrect values for range [-8,-4)!",correct,fe.frequentBetween(-8, -4, true, false));
        correct = Arrays.asList(new TestFrequency(-4,7,0));
        assertMatches("Incorrect values for range (-8,-4]!",correct,fe.frequentBetween(-8, -4, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (-8,-4)!",correct,fe.frequentBetween(-8, -4, false, false));

        //check from -4 to 0
        correct = Arrays.asList(new TestFrequency(-4,7,0),new TestFrequency(0,15,0));
        assertMatches("Incorrect values for range [-4,0]!",correct,fe.frequentBetween(-4, 0, true, true));
        correct = Arrays.asList(new TestFrequency(-4,7,0));
        assertMatches("Incorrect values for range [-4,0)!",correct,fe.frequentBetween(-4, 0, true, false));
        correct = Arrays.asList(new TestFrequency(0,15,0));
        assertMatches("Incorrect values for range (-4,0]!",correct,fe.frequentBetween(-4, 0, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (-4,0]!",correct,fe.frequentBetween(-4, 0, false, false));

        //check from 0 to 4
        correct = Arrays.asList(new TestFrequency(4,7,0),new TestFrequency(0,15,0));
        assertMatches("Incorrect values for range [0,4]!",correct,fe.frequentBetween(0,4,true,true));
        correct = Arrays.asList(new TestFrequency(0,15,0));
        assertMatches("Incorrect values for range [0,4)!",correct,fe.frequentBetween(0, 4, true, false));
        correct = Arrays.asList(new TestFrequency(4,7,0));
        assertMatches("Incorrect values for range (0,4]!",correct,fe.frequentBetween(0, 4, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (0,4)!",correct,fe.frequentBetween(0, 4, false, false));

        //check from 4 to 8
        correct = Arrays.asList(new TestFrequency(4,7,0));
        assertMatches("Incorrect values for range [4,8]!",correct,fe.frequentBetween(4, 8, true, true));
        assertMatches("Incorrect values for range [4,8)!",correct,fe.frequentBetween(4, 8, true, false));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (4,8]!",correct,fe.frequentBetween(4, 8, false, true));
        assertMatches("Incorrect values for range (4,8)!",correct,fe.frequentBetween(4, 8, false, false));

        //check from -8 to 0
        correct = Arrays.asList(new TestFrequency(-8,15,0),new TestFrequency(-4,7,0),new TestFrequency(0,15,0));
        assertMatches("Incorrect values for range [-8,0]!",correct,fe.frequentBetween(-8, 0, true, true));
        correct = Arrays.asList(new TestFrequency(-8,15,0),new TestFrequency(-4,7,0));
        assertMatches("Incorrect values for range [-8,0)!",correct,fe.frequentBetween(-8, 0, true, false));
        correct = Arrays.asList(new TestFrequency(0,15,0),new TestFrequency(-4,7,0));
        assertMatches("Incorrect values for range (-8,0]!",correct,fe.frequentBetween(-8, 0, false, true));
        correct = Arrays.asList(new TestFrequency(-4,7,0));
        assertMatches("Incorrect values for range (-8,0]!",correct,fe.frequentBetween(-8, 0, false, false));

        //check from 0 to 8
        correct = Arrays.asList(new TestFrequency(4,7,0),new TestFrequency(0,15,0));
        assertMatches("Incorrect values for range [0,8]!",correct,fe.frequentBetween(0, 8, true, true));
        assertMatches("Incorrect values for range [0,8)!",correct,fe.frequentBetween(0, 8, true, false));
        correct = Arrays.asList(new TestFrequency(4,7,0));
        assertMatches("Incorrect values for range (0,8]!",correct,fe.frequentBetween(0, 8, false, true));
        assertMatches("Incorrect values for range (0,8]!",correct,fe.frequentBetween(0, 8, false, false));
    }

    private void assertMatches(String message, List<TestFrequency> correct,
                               Collection<? extends LongFrequencyEstimate> actual){
        Assert.assertEquals(message+":incorrect size!",correct.size(),actual.size());
        List<LongFrequencyEstimate> actualEst = Lists.newArrayList(actual);
        Comparator<LongFrequencyEstimate> c1 = new Comparator<LongFrequencyEstimate>() {
            @Override
            public int compare(LongFrequencyEstimate o1, LongFrequencyEstimate o2) {
                return (int) (o1.getValue() - o2.getValue());
            }
        };
        Collections.sort(correct, c1);
        Collections.sort(actualEst, c1);
        for(int i=0;i<correct.size();i++){
            LongFrequencyEstimate c = correct.get(i);
            LongFrequencyEstimate a = actualEst.get(i);
            Assert.assertEquals(message+":Incorrect estimate at pos "+ i,c,a);
//            Assert.assertEquals(message+":Incorrect value at pos "+ i,c.getValue(),a.getValue());
//            Assert.assertEquals(message+":Incorrect count at pos "+ i,c.count(),a.count());
//            Assert.assertEquals(message+":Incorrect error at pos "+ i,c.error(),a.error());
        }
    }

    private void assertEmpty(String message,Set<? extends LongFrequencyEstimate> frequencyEstimates){
        Assert.assertEquals(message, 0, frequencyEstimates.size());
        Assert.assertFalse(message + ":Iterator claims to hasNext()", frequencyEstimates.iterator().hasNext());
    }

    private void fillPowersOf2(LongFrequencyCounter counter) {
        for(int i=-8;i<8;i++){
            counter.update(i);
            if(i%2==0) counter.update(i,2);
            if(i%4==0) counter.update(i,4);
            if(i%8==0) counter.update(i,8);
        }
    }

    private static class TestFrequency implements LongFrequencyEstimate{
        final long value;
        long count;
        long error;

        public TestFrequency(long value, long count,long error) {
            this.value = value;
            this.count = count;
            this.error = error;
        }

        @Override
        public int compareTo(LongFrequencyEstimate o) {
            return Longs.compare(o.value(),value);
        }

        @Override public long value() { return value; }
        @Override public Long getValue() { return value; }
        @Override public long count() { return count; }
        @Override public long error() { return error; }

        @Override
        public FrequencyEstimate<Long> merge(FrequencyEstimate<Long> other) {
            count+=other.count();
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FrequencyEstimate)) return false;

            @SuppressWarnings("unchecked") FrequencyEstimate<Long> that = (FrequencyEstimate<Long>) o;

            if(value!=that.getValue()) return false;
            long guaranteedValue = count-error;
            long otherGV = that.count()-that.error();

            return guaranteedValue==otherGV;
        }

        @Override
        public int hashCode() {
            int result = Longs.hashCode(value);
            result = 31 * result + (int) (count ^ (count >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "Frequency("+value+","+count+")";
        }
    }
}
