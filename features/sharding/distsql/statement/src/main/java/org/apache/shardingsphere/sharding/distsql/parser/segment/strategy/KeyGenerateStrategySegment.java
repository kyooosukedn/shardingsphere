/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.sharding.distsql.parser.segment.strategy;

import lombok.Getter;
import org.apache.shardingsphere.distsql.parser.segment.AlgorithmSegment;
import org.apache.shardingsphere.sql.parser.api.ASTNode;

import java.util.Optional;

/**
 * Key generate strategy segment.
 */
@Getter
public final class KeyGenerateStrategySegment implements ASTNode {
    
    private final String keyGenerateColumn;
    
    private final Optional<String> keyGenerateAlgorithmName;
    
    private AlgorithmSegment keyGenerateAlgorithmSegment;
    
    public KeyGenerateStrategySegment(final String keyGenerateColumn, final String keyGenerateAlgorithmName) {
        this.keyGenerateColumn = keyGenerateColumn;
        this.keyGenerateAlgorithmName = Optional.ofNullable(keyGenerateAlgorithmName);
    }
    
    public KeyGenerateStrategySegment(final String keyGenerateColumn, final AlgorithmSegment keyGenerateAlgorithmSegment) {
        this.keyGenerateColumn = keyGenerateColumn;
        this.keyGenerateAlgorithmName = Optional.empty();
        this.keyGenerateAlgorithmSegment = keyGenerateAlgorithmSegment;
    }
}
