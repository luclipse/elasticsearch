/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common;

/**
 */
public final class CompareUtils {

    private CompareUtils() {
    }

    public static int compare(byte left, byte right) {
        return ((int) left) - ((int) right);
    }

    public static int compare(short left, short right) {
        return ((int) left) - ((int) right);
    }

    public static int compare(int left, int right) {
        return left > right ? 1 : left == right ? 0 : -1;
    }

    public static int compare(long left, long right) {
        return left > right ? 1 : left == right ? 0 : -1;
    }

    public static int compare(float left, float right) {
        return left > right ? 1 : left == right ? 0 : -1;
    }

    public static int compare(double left, double right) {
        return left > right ? 1 : left == right ? 0 : -1;
    }

    public static boolean smallerThan(long left, long right) {
        return compare(left, right) < 0;
    }

    public static boolean largerThan(long left, long right) {
        return compare(left, right) > 0;
    }

}
