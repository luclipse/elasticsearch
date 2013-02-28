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

package org.elasticsearch.common.lucene;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class Lucene {

    public static final Version VERSION = Version.LUCENE_41;
    public static final Version ANALYZER_VERSION = VERSION;
    public static final Version QUERYPARSER_VERSION = VERSION;

    public static final NamedAnalyzer STANDARD_ANALYZER = new NamedAnalyzer("_standard", AnalyzerScope.GLOBAL, new StandardAnalyzer(ANALYZER_VERSION));
    public static final NamedAnalyzer KEYWORD_ANALYZER = new NamedAnalyzer("_keyword", AnalyzerScope.GLOBAL, new KeywordAnalyzer());

    public static final int NO_DOC = -1;

    public static ScoreDoc[] EMPTY_SCORE_DOCS = new ScoreDoc[0];

    public static Version parseVersion(@Nullable String version, Version defaultVersion, ESLogger logger) {
        if (version == null) {
            return defaultVersion;
        }
        if ("4.1".equals(version)) {
            return Version.LUCENE_41;
        }
        if ("4.0".equals(version)) {
            return Version.LUCENE_40;
        }
        if ("3.6".equals(version)) {
            return Version.LUCENE_36;
        }
        if ("3.5".equals(version)) {
            return Version.LUCENE_35;
        }
        if ("3.4".equals(version)) {
            return Version.LUCENE_34;
        }
        if ("3.3".equals(version)) {
            return Version.LUCENE_33;
        }
        if ("3.2".equals(version)) {
            return Version.LUCENE_32;
        }
        if ("3.1".equals(version)) {
            return Version.LUCENE_31;
        }
        if ("3.0".equals(version)) {
            return Version.LUCENE_30;
        }
        logger.warn("no version match {}, default to {}", version, defaultVersion);
        return defaultVersion;
    }

    /**
     * Reads the segments infos, failing if it fails to load
     */
    public static SegmentInfos readSegmentInfos(Directory directory) throws IOException {
        final SegmentInfos sis = new SegmentInfos();
        sis.read(directory);
        return sis;
    }

    public static long count(IndexSearcher searcher, Query query) throws IOException {
        TotalHitCountCollector countCollector = new TotalHitCountCollector();
        // we don't need scores, so wrap it in a constant score query
        if (!(query instanceof ConstantScoreQuery)) {
            query = new ConstantScoreQuery(query);
        }
        searcher.search(query, countCollector);
        return countCollector.getTotalHits();
    }

    /**
     * Closes the index writer, returning <tt>false</tt> if it failed to close.
     */
    public static boolean safeClose(IndexWriter writer) {
        if (writer == null) {
            return true;
        }
        try {
            writer.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static TopDocs readTopDocs(StreamInput in) throws IOException {
        if (!in.readBoolean()) {
            // no docs
            return null;
        }
        if (in.readBoolean()) {
            int totalHits = in.readVInt();
            float maxScore = in.readFloat();

            SortField[] fields = new SortField[in.readVInt()];
            for (int i = 0; i < fields.length; i++) {
                String field = null;
                if (in.readBoolean()) {
                    field = in.readString();
                }
                fields[i] = new SortField(field, readSortType(in), in.readBoolean());
            }

            FieldDoc[] fieldDocs = new FieldDoc[in.readVInt()];
            for (int i = 0; i < fieldDocs.length; i++) {
                Comparable[] cFields = new Comparable[in.readVInt()];
                for (int j = 0; j < cFields.length; j++) {
                    byte type = in.readByte();
                    if (type == 0) {
                        cFields[j] = null;
                    } else if (type == 1) {
                        cFields[j] = in.readString();
                    } else if (type == 2) {
                        cFields[j] = in.readInt();
                    } else if (type == 3) {
                        cFields[j] = in.readLong();
                    } else if (type == 4) {
                        cFields[j] = in.readFloat();
                    } else if (type == 5) {
                        cFields[j] = in.readDouble();
                    } else if (type == 6) {
                        cFields[j] = in.readByte();
                    } else if (type == 7) {
                        cFields[j] = in.readShort();
                    } else if (type == 8) {
                        cFields[j] = in.readBoolean();
                    } else if (type == 9) {
                        cFields[j] = in.readBytesRef();
                    } else {
                        throw new IOException("Can't match type [" + type + "]");
                    }
                }
                fieldDocs[i] = new FieldDoc(in.readVInt(), in.readFloat(), cFields);
            }
            return new TopFieldDocs(totalHits, fieldDocs, fields, maxScore);
        } else {
            int totalHits = in.readVInt();
            float maxScore = in.readFloat();

            ScoreDoc[] scoreDocs = new ScoreDoc[in.readVInt()];
            for (int i = 0; i < scoreDocs.length; i++) {
                scoreDocs[i] = new ScoreDoc(in.readVInt(), in.readFloat());
            }
            return new TopDocs(totalHits, scoreDocs, maxScore);
        }
    }

    public static void writeTopDocs(StreamOutput out, TopDocs topDocs, int from) throws IOException {
        if (topDocs.scoreDocs.length - from < 0) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        if (topDocs instanceof TopFieldDocs) {
            out.writeBoolean(true);
            TopFieldDocs topFieldDocs = (TopFieldDocs) topDocs;

            out.writeVInt(topDocs.totalHits);
            out.writeFloat(topDocs.getMaxScore());

            out.writeVInt(topFieldDocs.fields.length);
            for (SortField sortField : topFieldDocs.fields) {
                if (sortField.getField() == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    out.writeString(sortField.getField());
                }
                if (sortField.getComparatorSource() != null) {
                    writeSortType(out, ((IndexFieldData.XFieldComparatorSource) sortField.getComparatorSource()).reducedType());
                } else {
                    writeSortType(out, sortField.getType());
                }
                out.writeBoolean(sortField.getReverse());
            }

            out.writeVInt(topDocs.scoreDocs.length - from);
            int index = 0;
            for (ScoreDoc doc : topFieldDocs.scoreDocs) {
                if (index++ < from) {
                    continue;
                }
                FieldDoc fieldDoc = (FieldDoc) doc;
                out.writeVInt(fieldDoc.fields.length);
                for (Object field : fieldDoc.fields) {
                    if (field == null) {
                        out.writeByte((byte) 0);
                    } else {
                        Class type = field.getClass();
                        if (type == String.class) {
                            out.writeByte((byte) 1);
                            out.writeString((String) field);
                        } else if (type == Integer.class) {
                            out.writeByte((byte) 2);
                            out.writeInt((Integer) field);
                        } else if (type == Long.class) {
                            out.writeByte((byte) 3);
                            out.writeLong((Long) field);
                        } else if (type == Float.class) {
                            out.writeByte((byte) 4);
                            out.writeFloat((Float) field);
                        } else if (type == Double.class) {
                            out.writeByte((byte) 5);
                            out.writeDouble((Double) field);
                        } else if (type == Byte.class) {
                            out.writeByte((byte) 6);
                            out.writeByte((Byte) field);
                        } else if (type == Short.class) {
                            out.writeByte((byte) 7);
                            out.writeShort((Short) field);
                        } else if (type == Boolean.class) {
                            out.writeByte((byte) 8);
                            out.writeBoolean((Boolean) field);
                        } else if (type == BytesRef.class) {
                            out.writeByte((byte) 9);
                            out.writeBytesRef((BytesRef) field);
                        } else {
                            throw new IOException("Can't handle sort field value of type [" + type + "]");
                        }
                    }
                }

                out.writeVInt(doc.doc);
                out.writeFloat(doc.score);
            }
        } else {
            out.writeBoolean(false);
            out.writeVInt(topDocs.totalHits);
            out.writeFloat(topDocs.getMaxScore());

            out.writeVInt(topDocs.scoreDocs.length - from);
            int index = 0;
            for (ScoreDoc doc : topDocs.scoreDocs) {
                if (index++ < from) {
                    continue;
                }
                out.writeVInt(doc.doc);
                out.writeFloat(doc.score);
            }
        }
    }

    // LUCENE 4 UPGRADE: We might want to maintain our own ordinal, instead of Lucene's ordinal
    public static SortField.Type readSortType(StreamInput in) throws IOException {
        return SortField.Type.values()[in.readVInt()];
    }

    public static void writeSortType(StreamOutput out, SortField.Type sortType) throws IOException {
        out.writeVInt(sortType.ordinal());
    }

    public static Explanation readExplanation(StreamInput in) throws IOException {
        float value = in.readFloat();
        String description = in.readString();
        Explanation explanation = new Explanation(value, description);
        if (in.readBoolean()) {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                explanation.addDetail(readExplanation(in));
            }
        }
        return explanation;
    }

    public static void writeExplanation(StreamOutput out, Explanation explanation) throws IOException {
        out.writeFloat(explanation.getValue());
        out.writeString(explanation.getDescription());
        Explanation[] subExplanations = explanation.getDetails();
        if (subExplanations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(subExplanations.length);
            for (Explanation subExp : subExplanations) {
                writeExplanation(out, subExp);
            }
        }
    }

    public static <T> Collection<SearchGroup<T>> readSearchGroups(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<SearchGroup<T>> searchGroups = new ArrayList<SearchGroup<T>>(size);
        for (int i = 0; i < size; i++) {
            SearchGroup<T> sg = new SearchGroup<T>();
            sg.groupValue = (T) readSortValue(in);
            int sortSize = in.readVInt();
            sg.sortValues = new Object[sortSize];
            for (int j = 0; j < sortSize; j++) {
                sg.sortValues[j] = readSortValue(in);
            }
            searchGroups.add(sg);
        }
        return searchGroups;
    }

    public static <T> void writeSearchGroups(Collection<SearchGroup<T>> searchGroups, StreamOutput out) throws IOException {
        out.writeVInt(searchGroups.size());
        for (SearchGroup<T> searchGroup : searchGroups) {
            writeSortValue(searchGroup.groupValue, out);
            out.writeVInt(searchGroup.sortValues.length);
            for (Object sortValue : searchGroup.sortValues) {
                writeSortValue(sortValue, out);
            }
        }
    }

    public static <T> void writeTopGroups(TopGroups<T> topGroups, StreamOutput out) throws IOException {
        out.writeVInt(topGroups.totalHitCount);
        out.writeFloat(topGroups.maxScore);
        out.writeVInt(topGroups.groups.length);
        for (GroupDocs<T> group : topGroups.groups) {
            writeSortValue(group.groupValue, out);
            out.writeVInt(group.totalHits);
            out.writeFloat(group.maxScore);

            out.writeVInt(group.groupSortValues.length);
            for (Object groupSortValue : group.groupSortValues) {
                writeSortValue(groupSortValue, out);
            }

            out.writeVInt(group.scoreDocs.length);
            for (ScoreDoc doc : group.scoreDocs) {
                out.writeVInt(doc.doc);
                out.writeFloat(doc.score);
                if (doc instanceof FieldDoc) {
                    out.writeBoolean(true);
                    FieldDoc fieldDoc = (FieldDoc) doc;
                    out.writeVInt(fieldDoc.fields.length);
                    for (Object field : fieldDoc.fields) {
                        writeSortValue(field, out);
                    }
                } else {
                    out.writeBoolean(false);
                }
            }
        }
    }

    public static <T> TopGroups<T> readTopGroups(StreamInput in) throws IOException {
        int totalHitCount = in.readVInt();
        float maxScore = in.readFloat();
        int groupSize = in.readVInt();
        GroupDocs[] groups = new GroupDocs[groupSize];
        for (int i = 0; i < groupSize; i++) {
            Object groupValue = readSortValue(in);
            int totalHits = in.readVInt();
            float groupMaxScore = in.readFloat();

            int groupSortSize = in.readVInt();
            Object[] groupSortValues = new Object[groupSortSize];
            for (int j = 0; j < groupSortSize; j++) {
                groupSortValues[j] = readSortValue(in);
            }

            int docsSize = in.readVInt();
            ScoreDoc[] docs = new ScoreDoc[docsSize];
            for (int j = 0; j < docsSize; j++) {
                int docId = in.readVInt();
                float score = in.readFloat();
                if (in.readBoolean()) {
                    int sortSize = in.readVInt();
                    Object[] fields = new Object[sortSize];
                    for (int k = 0; k < sortSize; k++) {
                        fields[k] = readSortValue(in);
                    }
                    docs[j] = new FieldDoc(docId, score, fields);
                } else {
                    docs[j] = new ScoreDoc(docId, score);
                }
            }
            groups[i] = new GroupDocs(Float.NaN, groupMaxScore, totalHits, docs, groupValue, groupSortValues);
        }

        return new TopGroups<T>(null, null, totalHitCount, -1, groups, maxScore);
    }

    private static void writeSortValue(Object value, StreamOutput out) throws IOException {
        if (value == null) {
            out.writeByte((byte) 0);
        } else {
            Class type = value.getClass();
            if (type == String.class) {
                out.writeByte((byte) 1);
                out.writeString((String) value);
            } else if (type == Integer.class) {
                out.writeByte((byte) 2);
                out.writeInt((Integer) value);
            } else if (type == Long.class) {
                out.writeByte((byte) 3);
                out.writeLong((Long) value);
            } else if (type == Float.class) {
                out.writeByte((byte) 4);
                out.writeFloat((Float) value);
            } else if (type == Double.class) {
                out.writeByte((byte) 5);
                out.writeDouble((Double) value);
            } else if (type == Byte.class) {
                out.writeByte((byte) 6);
                out.writeByte((Byte) value);
            } else if (type == Short.class) {
                out.writeByte((byte) 7);
                out.writeShort((Short) value);
            } else if (type == Boolean.class) {
                out.writeByte((byte) 8);
                out.writeBoolean((Boolean) value);
            } else if (type == BytesRef.class) {
                out.writeByte((byte) 9);
                out.writeBytesRef((BytesRef) value);
            } else {
                throw new IOException("Can't handle sort field value of type [" + type + "]");
            }
        }
    }

    private static Object readSortValue(StreamInput in) throws IOException {
        byte sortType = in.readByte();
        switch (sortType) {
            case 0:
                return null;
            case 1:
                return in.readString();
            case 2:
                return in.readInt();
            case 3:
                return in.readLong();
            case 4:
                return in.readFloat();
            case 5:
                return in.readDouble();
            case 6:
                return in.readByte();
            case 7:
                return in.readShort();
            case 8:
                return in.readBoolean();
            case 9:
                return in.readBytesRef();
            default:
                throw new IOException("Can't match sort type");
        }
    }

    public static void writeSort(Sort sort, StreamOutput out) throws IOException {
        out.writeVInt(sort.getSort().length);
        for (SortField sortField : sort.getSort()) {
            if (sortField.getField() == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(sortField.getField());
            }
            if (sortField.getComparatorSource() != null) {
                writeSortType(out, ((IndexFieldData.XFieldComparatorSource) sortField.getComparatorSource()).reducedType());
            } else {
                writeSortType(out, sortField.getType());
            }
            out.writeBoolean(sortField.getReverse());
        }
    }

    public static Sort readSort(StreamInput in) throws IOException {
        SortField[] fields = new SortField[in.readVInt()];
        for (int i = 0; i < fields.length; i++) {
            String field = null;
            if (in.readBoolean()) {
                field = in.readString();
            }
            fields[i] = new SortField(field, readSortType(in), in.readBoolean());
        }
        return new Sort(fields);
    }

    private static final Field segmentReaderSegmentInfoField;

    static {
        Field segmentReaderSegmentInfoFieldX = null;
        try {
            segmentReaderSegmentInfoFieldX = SegmentReader.class.getDeclaredField("si");
            segmentReaderSegmentInfoFieldX.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        segmentReaderSegmentInfoField = segmentReaderSegmentInfoFieldX;
    }

    public static SegmentInfoPerCommit getSegmentInfo(SegmentReader reader) {
        try {
            return (SegmentInfoPerCommit) segmentReaderSegmentInfoField.get(reader);
        } catch (IllegalAccessException e) {
            return null;
        }
    }

    public static class ExistsCollector extends Collector {

        private boolean exists;

        public void reset() {
            exists = false;
        }

        public boolean exists() {
            return exists;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.exists = false;
        }

        @Override
        public void collect(int doc) throws IOException {
            exists = true;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }
    }

    private Lucene() {

    }
}
