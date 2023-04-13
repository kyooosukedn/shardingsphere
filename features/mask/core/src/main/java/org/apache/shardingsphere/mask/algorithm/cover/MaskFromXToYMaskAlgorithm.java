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

import com.google.common.base.Strings;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.mask.algorithm.MaskAlgorithmPropsChecker;
import org.apache.shardingsphere.mask.exception.algorithm.MaskAlgorithmInitializationException;
import org.apache.shardingsphere.mask.spi.MaskAlgorithm;

import java.util.Properties;

/**
 * Mask from x to y mask algorithm.
 */
public final class MaskFromXToYMaskAlgorithm implements MaskAlgorithm<Object, String> {
    
    private static final String FROM_X = "from-x";
    
    private static final String TO_Y = "to-y";
    
    private static final String REPLACE_CHAR = "replace-char";
    
    private Integer fromX;
    
    private Integer toY;
    
    private Character replaceChar;
    
    @Override
    public void init(final Properties props) {
        fromX = createFromX(props);
        toY = createToY(props);
        replaceChar = createReplaceChar(props);
    }
    
    private Integer createFromX(final Properties props) {
        MaskAlgorithmPropsChecker.checkIntegerTypeConfig(props, FROM_X, getType());
        String fromXValue = props.getProperty(FROM_X);
        if (!Strings.isNullOrEmpty(fromXValue)) {
            fromX = Integer.parseInt(fromXValue);
            ShardingSpherePreconditions.checkState(fromX > 0, () -> new MaskAlgorithmInitializationException(getType(), "from-X must be a positive integer."));
        }
        return fromX;
    }
    
    private Integer createToY(final Properties props) {
        MaskAlgorithmPropsChecker.checkIntegerTypeConfig(props, TO_Y, getType());
        String toYValue = props.getProperty(TO_Y);
        if (!Strings.isNullOrEmpty(toYValue)) {
            toY = Integer.parseInt(toYValue);
            ShardingSpherePreconditions.checkState(toY > 0, () -> new MaskAlgorithmInitializationException(getType(), "to-Y must be a positive integer."));
        }
        return toY;
    }
    
    private Character createReplaceChar(final Properties props) {
        String replaceCharStr = props.getProperty(REPLACE_CHAR);
        MaskAlgorithmPropsChecker.checkRequiredPropertyConfig(props, REPLACE_CHAR, getType());
        MaskAlgorithmPropsChecker.checkSingleCharConfig(props, REPLACE_CHAR, getType());
        return replaceCharStr.charAt(0);
    }
    
    @Override
    public String mask(final Object plainValue) {
        String result = null == plainValue ? null : String.valueOf(plainValue);
        if (Strings.isNullOrEmpty(result)) {
            return result;
        }
        if (result.length() <= fromX || toY < fromX) {
            return result;
        }
        char[] chars = result.toCharArray();
        for (int i = fromX, minLength = Math.min(toY, chars.length - 1); i <= minLength; i++) {
            chars[i] = replaceChar;
        }
        return new String(chars);
    }
    
    @Override
    public String getType() {
        return "MASK_FROM_X_TO_Y";
    }
}
