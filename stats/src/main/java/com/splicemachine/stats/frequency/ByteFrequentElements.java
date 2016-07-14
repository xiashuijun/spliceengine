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

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface ByteFrequentElements extends FrequentElements<Byte> {

    ByteFrequencyEstimate countEqual(byte item);

    Set<ByteFrequencyEstimate> frequentBetween(byte start, byte stop, boolean includeMid, boolean includeMax);

    Set<ByteFrequencyEstimate> frequentAfter(byte start, boolean includeStart);

    Set<ByteFrequencyEstimate> frequentBefore(byte stop, boolean includeStop);

    ByteFrequentElements getClone();
}
