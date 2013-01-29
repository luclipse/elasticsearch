/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.lucene.search.postingshighlight;

import java.text.BreakIterator;
import java.text.CharacterIterator;

/**
 * A <code>BreakIterator</code> that doesn't break up text. This allows us to highlight a complete field in one fragment.
 * This is expected behaviour when setting `number_of_fragments` to `0`.
 */
public class NoneBreakIterator extends BreakIterator {

    private String text;

    @Override
    public int first() {
        return 0;
    }

    @Override
    public int last() {
        return 0;
    }

    @Override
    public int next(int n) {
        return 0;
    }

    @Override
    public int next() {
        return text.length();
    }

    @Override
    public int preceding(int offset) {
        return 0;
    }

    @Override
    public int previous() {
        return 0;
    }

    @Override
    public int following(int offset) {
        return 0;
    }

    @Override
    public int current() {
        return 0;
    }

    @Override
    public CharacterIterator getText() {
        return null;
    }

    @Override
    public void setText(String newText) {
        this.text = newText;
    }

    @Override
    public void setText(CharacterIterator newText) {
    }

}
