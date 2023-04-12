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

package org.apache.shardingsphere.mask.algorithm.cover;

import org.apache.shardingsphere.mask.exception.algorithm.MaskAlgorithmInitializationException;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KeepFromXToYMaskAlgorithmTest {
    
    private KeepFromXToYMaskAlgorithm maskAlgorithm;
    
    @BeforeEach
    void setUp() {
        maskAlgorithm = new KeepFromXToYMaskAlgorithm();
        maskAlgorithm.init(PropertiesBuilder.build(new Property("from-x", "2"), new Property("to-y", "5"), new Property("replace-char", "*")));
    }
    
    @Test
    void assertMask() {
        assertThat(maskAlgorithm.mask("abc123456"), is("**c123***"));
    }
    
    @Test
    void assertMaskWhenPlainValueLengthLessThanToY() {
        assertThat(maskAlgorithm.mask("abc"), is("**c"));
    }
    
    @Test
    void assertMaskWhenPlainValueLengthLessThanFromX() {
        assertThat(maskAlgorithm.mask("a"), is("a"));
    }
    
    @Test
    void assertInitWhenFromXIsEmpty() {
        assertThrows(MaskAlgorithmInitializationException.class,
                () -> new KeepFirstNLastMMaskAlgorithm().init(PropertiesBuilder.build(new Property("from-x", ""), new Property("to-y", "5"), new Property("replace-char", "*"))));
    }
    
    @Test
    void assertInitWhenToYIsEmpty() {
        assertThrows(MaskAlgorithmInitializationException.class,
                () -> new KeepFirstNLastMMaskAlgorithm().init(PropertiesBuilder.build(new Property("from-x", "2"), new Property("to-y", ""), new Property("replace-char", "*"))));
    }
    
    @Test
    void assertInitWhenReplaceCharIsEmpty() {
        assertThrows(MaskAlgorithmInitializationException.class,
                () -> new KeepFirstNLastMMaskAlgorithm().init(PropertiesBuilder.build(new Property("from-x", "2"), new Property("to-y", "5"), new Property("replace-char", ""))));
    }
    
    @Test
    void assertInitWhenFromXIsNotPositive() {
        assertThrows(MaskAlgorithmInitializationException.class,
                () -> new MaskFirstNLastMMaskAlgorithm().init(PropertiesBuilder.build(new Property("from-x", "-3"), new Property("to-y", "5"), new Property("replace-char", "*"))));
    }
    
    @Test
    void assertInitWhenToYIsNotPositive() {
        assertThrows(MaskAlgorithmInitializationException.class,
                () -> new MaskFirstNLastMMaskAlgorithm().init(PropertiesBuilder.build(new Property("from-x", "3"), new Property("to-y", "-5"), new Property("replace-char", "*"))));
    }
}
