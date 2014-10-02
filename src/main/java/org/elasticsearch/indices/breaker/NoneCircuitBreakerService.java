/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * Class that returns a breaker that never breaks
 */
public class NoneCircuitBreakerService extends CircuitBreakerService {

    private final ESLogger logger = Loggers.getLogger(NoneCircuitBreakerService.class);

    private final MemoryCircuitBreaker breaker = new MemoryCircuitBreaker(new ByteSizeValue(Long.MAX_VALUE), 0.0, logger);

    public NoneCircuitBreakerService() {
        super(ImmutableSettings.EMPTY);
    }


    @Override
    public MemoryCircuitBreaker getBreaker(CircuitBreaker.Name name) {
        return breaker;
    }

    @Override
    public AllCircuitBreakerStats stats() {
        return new AllCircuitBreakerStats(new CircuitBreakerStats[] {stats(CircuitBreaker.Name.FIELDDATA)});
    }

    @Override
    public CircuitBreakerStats stats(CircuitBreaker.Name name) {
        return new CircuitBreakerStats(CircuitBreaker.Name.FIELDDATA, -1, -1, 0, 0);
    }
}