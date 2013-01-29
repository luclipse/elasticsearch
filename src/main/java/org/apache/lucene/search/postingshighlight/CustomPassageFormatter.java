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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Allows us to have access to the {@link Passage} instances.
 * We don't want that the passages are concatenated into one string.
 * We can retrieve the passages with this manner, because we highlight one document at the time.
 */
public class CustomPassageFormatter extends PassageFormatter {

    private final String preTag;
    private final String postTag;
    private final List<String> passages;

    public CustomPassageFormatter(String preTag, String postTag) {
        this.preTag = preTag;
        this.postTag = postTag;
        this.passages = new ArrayList<String>();
    }

    @Override
    public String format(Passage[] passages, String content) {
        for (Passage passage : passages) {
            // Hack, need to fix this properly. Matches in passage can be out of order. Happens when with texts like this:
            // "a b a". The second "a" will be placed before "b"
            Arrays.sort(passage.matchStarts, 0, passage.numMatches);
            Arrays.sort(passage.matchEnds, 0, passage.numMatches);

            StringBuilder sb = new StringBuilder();
            int pos = passage.startOffset;
            for (int i = 0; i < passage.numMatches; i++) {
                int start = passage.matchStarts[i];
                int end = passage.matchEnds[i];
                // its possible to have overlapping terms
                if (start > pos) {
                    sb.append(content.substring(pos, start));
                }
                if (end > pos) {
                    sb.append(preTag);
                    sb.append(content.substring(Math.max(pos, start), end));
                    sb.append(postTag);
                    pos = end;
                }
            }
            // its possible a "term" from the analyzer could span a sentence boundary.
            sb.append(content.substring(pos, Math.max(pos, passage.endOffset)));
            this.passages.add(sb.toString());
        }

        return null;
    }

    public void clear() {
        passages.clear();
    }

    public String[] getPassages() {
        return passages.toArray(new String[passages.size()]);
    }
}
