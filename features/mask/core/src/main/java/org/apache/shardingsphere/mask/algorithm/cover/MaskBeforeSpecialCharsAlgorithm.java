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
import org.apache.shardingsphere.mask.algorithm.MaskAlgorithmPropsChecker;
import org.apache.shardingsphere.mask.spi.MaskAlgorithm;

import java.util.Properties;

/**
 * Mask before special-chars algorithm.
 */
public final class MaskBeforeSpecialCharsAlgorithm implements MaskAlgorithm<Object, String> {
    
    private static final String SPECIAL_CHARS = "special-chars";
    
    private static final String REPLACE_CHAR = "replace-char";
    
    private String specialChars;
    
    private Character replaceChar;
    
    @Override
    public void init(final Properties props) {
        specialChars = createSpecialChars(props);
        replaceChar = createReplaceChar(props);
    }
    
    private String createSpecialChars(final Properties props) {
        String specialChars = props.getProperty(SPECIAL_CHARS);
        MaskAlgorithmPropsChecker.checkRequiredPropertyConfig(props, SPECIAL_CHARS, getType());
        MaskAlgorithmPropsChecker.checkAtLeastOneCharConfig(props, SPECIAL_CHARS, getType());
        return specialChars;
    }
    
    private Character createReplaceChar(final Properties props) {
        final String replaceCharStr = props.getProperty(REPLACE_CHAR);
        MaskAlgorithmPropsChecker.checkRequiredPropertyConfig(props, REPLACE_CHAR, getType());
        MaskAlgorithmPropsChecker.checkSingleCharConfig(props, REPLACE_CHAR, getType());
        MaskAlgorithmPropsChecker.checkSingleCharConfig(props, REPLACE_CHAR, getType());
        return replaceCharStr.charAt(0);
    }
    
    @Override
    public String mask(final Object plainValue) {
        String result = null == plainValue ? null : String.valueOf(plainValue);
        if (Strings.isNullOrEmpty(result)) {
            return result;
        }
        int index = result.indexOf(specialChars);
        char[] chars = result.toCharArray();
        for (int i = 0; i < index; i++) {
            chars[i] = replaceChar;
        }
        return new String(chars);
    }
    
    @Override
    public String getType() {
        return "MASK_BEFORE_SPECIAL_CHARS";
    }

}
