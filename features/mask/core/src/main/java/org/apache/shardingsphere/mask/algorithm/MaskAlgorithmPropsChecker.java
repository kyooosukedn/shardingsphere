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

package org.apache.shardingsphere.mask.algorithm;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.mask.exception.algorithm.MaskAlgorithmInitializationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Mask algorithm props checker.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MaskAlgorithmPropsChecker {
    
    /**
     * Check single char config.
     *
     * @param props props
     * @param singleCharConfigKey single char config key
     * @param maskType mask type
     * @throws MaskAlgorithmInitializationException mask algorithm initialization exception
     */
    public static void checkSingleCharConfig(final Properties props, final String singleCharConfigKey, final String maskType) {
        ShardingSpherePreconditions.checkState(props.containsKey(singleCharConfigKey),
                () -> new MaskAlgorithmInitializationException(maskType, String.format("%s can not be null", singleCharConfigKey)));
        ShardingSpherePreconditions.checkState(1 == props.getProperty(singleCharConfigKey).length(),
                () -> new MaskAlgorithmInitializationException(maskType, String.format("%s's length must be one", singleCharConfigKey)));
    }
    
    /**
     * Check at least one char config.
     *
     * @param props props
     * @param atLeastOneCharConfigKey at least one char config key
     * @param maskType mask type
     * @throws MaskAlgorithmInitializationException mask algorithm initialization exception
     */
    public static void checkAtLeastOneCharConfig(final Properties props, final String atLeastOneCharConfigKey, final String maskType) {
        ShardingSpherePreconditions.checkState(props.containsKey(atLeastOneCharConfigKey),
                () -> new MaskAlgorithmInitializationException(maskType, String.format("%s can not be null", atLeastOneCharConfigKey)));
        ShardingSpherePreconditions.checkState(props.getProperty(atLeastOneCharConfigKey).length() > 0,
                () -> new MaskAlgorithmInitializationException(maskType, String.format("%s's length must be at least one", atLeastOneCharConfigKey)));
    }
    
    /**
     * Check integer type config.
     *
     * @param props props
     * @param integerTypeConfigKey integer type config key
     * @param maskType mask type
     * @throws MaskAlgorithmInitializationException mask algorithm initialization exception
     */
    public static void checkIntegerTypeConfig(final Properties props, final String integerTypeConfigKey, final String maskType) {
        ShardingSpherePreconditions.checkState(props.containsKey(integerTypeConfigKey),
                () -> new MaskAlgorithmInitializationException(maskType, String.format("%s can not be null", integerTypeConfigKey)));
        try {
            Integer.parseInt(props.getProperty(integerTypeConfigKey));
        } catch (final NumberFormatException ex) {
            throw new MaskAlgorithmInitializationException(maskType, String.format("%s must be a valid integer number", integerTypeConfigKey));
        }
    }
    
    /**
     * Check required property config.
     *
     * @param props props
     * @param requiredPropertyConfigKey required property config key
     * @param maskType mask type
     * @throws MaskAlgorithmInitializationException mask algorithm initialization exception
     */
    public static void checkRequiredPropertyConfig(final Properties props, final String requiredPropertyConfigKey, final String maskType) {
        ShardingSpherePreconditions.checkState(props.containsKey(requiredPropertyConfigKey),
                () -> new MaskAlgorithmInitializationException(maskType, String.format("%s is required", requiredPropertyConfigKey)));
    }

    /**
     * Check required property config.
     *
     * @param props props
     * @param positiveIntegerTypeConfigKey positive integer type config key
     * @param maskType mask type
     * @throws MaskAlgorithmInitializationException mask algorithm initialization exception
     */
    public static void checkPositiveIntegerConfig(final Properties props, final String positiveIntegerTypeConfigKey, final String maskType) {
        if (!Strings.isNullOrEmpty(positiveIntegerTypeConfigKey)) {
            int integerValue = Integer.parseInt(props.getProperty(positiveIntegerTypeConfigKey));
            ShardingSpherePreconditions.checkState(integerValue > 0,
                    () -> new MaskAlgorithmInitializationException(maskType, String.format("%s must be a positive integer.", positiveIntegerTypeConfigKey)));
        }
    }

    /**
     * Check if a property value is not empty.
     *
     * @param props props
     * @param propertyValue property value
     * @param allowedChars allowed chars
     * @param maskType mask type
     * @throws MaskAlgorithmInitializationException mask algorithm initialization exception
     */
    public static void checkPropertyValueContainsOnly(final Properties props, final String propertyValue, final List<Character> allowedChars, final String maskType) {
        ShardingSpherePreconditions.checkNotNull(props,
                () -> new MaskAlgorithmInitializationException(maskType, String.format("The value of property '%s' must not be null.", propertyValue)));
        for (char c : propertyValue.toCharArray()) {
            ShardingSpherePreconditions.checkState(allowedChars.contains(c),
                    () -> new MaskAlgorithmInitializationException(maskType, String.format("Invalid character '%s' in property '%s'. Allowed characters are %s.", c, propertyValue, allowedChars)));
        }
    }

    /**
     * Check if a property value contains only uppercase letters.
     *
     * @param props props
     * @param upperCaseLetterConfigValue upper case letter config value
     * @param upperCaseLetterConfigKey upper case letter config key
     * @param maskType mask type
     * @throws MaskAlgorithmInitializationException mask algorithm initialization exception
     */
    public static void checkPropertyValueContainsOnlyUppercaseLetters(final Properties props, final String upperCaseLetterConfigValue, final String upperCaseLetterConfigKey, final String maskType) {
        List<Character> allowedChars = new ArrayList<>();
        if (upperCaseLetterConfigValue != null && !upperCaseLetterConfigValue.isEmpty()) {
            for (String s : upperCaseLetterConfigValue.split(",")) {
                allowedChars.add(s.charAt(0));
            }
        }
        checkPropertyValueContainsOnly(props, upperCaseLetterConfigValue, allowedChars, maskType);
    }

    /**
     * Check if a property value contains only lowercase letters.
     * @param props props
     * @param lowerCaseLetterConfigValue lower case letter config value
     * @param lowerCaseLetterConfigKey lower case letter config key
     * @param maskType mask type
     * @throws MaskAlgorithmInitializationException mask algorithm initialization exception
     */
    public static void checkPropertyValueContainsOnlyLowercaseLetters(final Properties props, final String lowerCaseLetterConfigValue, final String lowerCaseLetterConfigKey, final String maskType) {
        List<Character> allowedChars = new ArrayList<>();
        if (lowerCaseLetterConfigValue != null && !lowerCaseLetterConfigValue.isEmpty()) {
            for (String s : lowerCaseLetterConfigValue.split(",")) {
                allowedChars.add(s.charAt(0));
            }
        }
        checkPropertyValueContainsOnly(props, lowerCaseLetterConfigValue, allowedChars, maskType);
    }

    /**
     * Check if a property value contains only digits.
     * @param props props
     * @param onlyDigitsConfigValue only digits config value
     * @param maskType mask type
     * @throws MaskAlgorithmInitializationException if property contains invalid characters
     */
    public static void checkPropertyValueContainsOnlyDigits(final Properties props, final String onlyDigitsConfigValue, final String maskType) {
        List<Character> allowedChars = new ArrayList<>(Arrays.asList('0', '1', '2', '3', '4', '5', '6', '7', '8', '9'));
        checkPropertyValueContainsOnly(props, onlyDigitsConfigValue, allowedChars, maskType);
    }
}
