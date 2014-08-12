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
package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenSet;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.TypeParsers;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.parent;

/**
 *
 */
public class ParentFieldMapper extends AbstractFieldMapper<Uid> implements InternalMapper, RootMapper, DocumentTypeListener {

    public static final String NAME = "_parent";

    public static final String CONTENT_TYPE = "_parent";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = ParentFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            // TODO: index and store can be false now. Should we do this in 2.0?
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends Mapper.Builder<Builder, ParentFieldMapper> {

        private static final Settings FIELD_DATA_SETTINGS = ImmutableSettings.settingsBuilder()
                .put(Loading.KEY, Loading.EAGER_VALUE)
                .build();

        protected String indexName;

        private String type;
        protected PostingsFormatProvider postingsFormat;

        public Builder() {
            super(Defaults.NAME);
            this.indexName = name;
            builder = this;
        }

        public Builder type(String type) {
            this.type = type;
            return builder;
        }

        protected Builder postingsFormat(PostingsFormatProvider postingsFormat) {
            this.postingsFormat = postingsFormat;
            return builder;
        }

        @Override
        public ParentFieldMapper build(BuilderContext context) {
            if (type == null) {
                throw new MapperParsingException("Parent mapping must contain the parent type");
            }
            boolean docValues = context.indexCreatedVersion().onOrAfter(Version.V_1_4_0);
            return new ParentFieldMapper(name, indexName, type, postingsFormat, FIELD_DATA_SETTINGS, context.indexSettings(), docValues);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            ParentFieldMapper.Builder builder = parent();
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("type")) {
                    builder.type(fieldNode.toString());
                } else if (fieldName.equals("postings_format")) {
                    String postingFormatName = fieldNode.toString();
                    builder.postingsFormat(parserContext.postingFormatService().get(postingFormatName));
                }
            }
            return builder;
        }
    }

    private final String type;
    private final BytesRef typeAsBytes;
    private final boolean storeDocValues;
    private MapperService mapperService;
    private volatile ImmutableOpenSet<String> parentTypes;

    protected ParentFieldMapper(String name, String indexName, String type, PostingsFormatProvider postingsFormat, @Nullable Settings fieldDataSettings, Settings indexSettings, boolean storeDocValues) {
        super(new Names(name, indexName, indexName, name), Defaults.BOOST, new FieldType(Defaults.FIELD_TYPE), null,
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER, postingsFormat, null, null, null, fieldDataSettings, indexSettings);
        this.type = type;
        this.typeAsBytes = type == null ? null : new BytesRef(type);
        this.storeDocValues = storeDocValues;
    }

    public ParentFieldMapper() {
        this(Defaults.NAME, Defaults.NAME, null, null, null, null, false);
    }

    public String type() {
        return type;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("_parent");
    }

    @Override
    public boolean hasDocValues() {
        return storeDocValues;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        parse(context);
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!active()) {
            // If this is a parent document referred by a child doc that has doc values enabled in the _parent field
            // we need to save the id in the _parent doc values field
            for (DocumentMapper documentMapper : mapperService.docMappers(false)) {
                ParentFieldMapper parentFieldMapper = documentMapper.parentFieldMapper();
                if (parentFieldMapper.active() && parentFieldMapper.hasDocValues() && parentFieldMapper.parentTypes.contains(context.type())) {
                    fields.add(createParentIdField(context.type(), context.id()));
                    break;
                }
            }
            return;
        }

        String parentId = null;
        if (context.parser().currentName() != null && context.parser().currentName().equals(Defaults.NAME)) {
            // we are in the parsing of _parent phase
            parentId = context.parser().text();
            context.sourceToParse().parent(parentId);
            fields.add(new Field(names.indexName(), Uid.createUid(context.stringBuilder(), type, parentId), fieldType));
        } else {
            // otherwise, we are running it post processing of the xcontent
            String parsedParentId = context.doc().get(Defaults.NAME);
            if (context.sourceToParse().parent() != null) {
                parentId = context.sourceToParse().parent();
                if (parsedParentId == null) {
                    if (parentId == null) {
                        throw new MapperParsingException("No parent id provided, not within the document, and not externally");
                    }
                    // we did not add it in the parsing phase, add it now
                    fields.add(new Field(names.indexName(), Uid.createUid(context.stringBuilder(), type, parentId), fieldType));
                } else if (parentId != null && !parsedParentId.equals(Uid.createUid(context.stringBuilder(), type, parentId))) {
                    throw new MapperParsingException("Parent id mismatch, document value is [" + Uid.createUid(parsedParentId).id() + "], while external value is [" + parentId + "]");
                }
            }
        }
        if (hasDocValues() && parentId != null) {
            // Doc values is enabled for the _parent field, so we add an extra field
            fields.add(createParentIdField(type(), parentId));
        }

        // A document can be both child and parent:
        DocumentMapper documentMapper = mapperService.documentMapper(context.type());
        if (documentMapper.parentFieldMapper().active() && documentMapper.parentFieldMapper().hasDocValues() && parentTypes.contains(context.type())) {
            fields.add(createParentIdField(context.type(), context.id()));
        }
    }

    public static SortedDocValuesField createParentIdField(String parentType, String id) {
        String fieldName = ParentFieldMapper.NAME + "#" + parentType;
        return new SortedDocValuesField(fieldName, new BytesRef(id));
    }

    @Override
    public Uid value(Object value) {
        if (value == null) {
            return null;
        }
        return Uid.createUid(value.toString());
    }

    @Override
    public Object valueForSearch(Object value) {
        if (value == null) {
            return null;
        }
        String sValue = value.toString();
        if (sValue == null) {
            return null;
        }
        int index = sValue.indexOf(Uid.DELIMITER);
        if (index == -1) {
            return sValue;
        }
        return sValue.substring(index + 1);
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        if (value instanceof BytesRef) {
            BytesRef bytesRef = (BytesRef) value;
            if (Uid.hasDelimiter(bytesRef)) {
                return bytesRef;
            }
            return Uid.createUidAsBytes(typeAsBytes, bytesRef);
        }
        String sValue = value.toString();
        if (sValue.indexOf(Uid.DELIMITER) == -1) {
            return Uid.createUidAsBytes(type, sValue);
        }
        return super.indexedValueForSearch(value);
    }

    @Override
    public Query termQuery(Object value, @Nullable QueryParseContext context) {
        if (context == null) {
            return super.termQuery(value, context);
        }
        return new ConstantScoreQuery(termFilter(value, context));
    }

    @Override
    public Filter termFilter(Object value, @Nullable QueryParseContext context) {
        if (context == null) {
            return super.termFilter(value, context);
        }
        BytesRef bValue = BytesRefs.toBytesRef(value);
        if (Uid.hasDelimiter(bValue)) {
            return new TermFilter(new Term(names.indexName(), bValue));
        }

        List<String> types = new ArrayList<>(context.mapperService().types().size());
        for (DocumentMapper documentMapper : context.mapperService().docMappers(false)) {
            if (!documentMapper.parentFieldMapper().active()) {
                types.add(documentMapper.type());
            }
        }

        if (types.isEmpty()) {
            return Queries.MATCH_NO_FILTER;
        } else if (types.size() == 1) {
            return new TermFilter(new Term(names.indexName(), Uid.createUidAsBytes(types.get(0), bValue)));
        } else {
            // we use all non child types, cause we don't know if its exact or not...
            List<BytesRef> typesValues = new ArrayList<>(types.size());
            for (String type : context.mapperService().types()) {
                typesValues.add(Uid.createUidAsBytes(type, bValue));
            }
            return new TermsFilter(names.indexName(), typesValues);
        }
    }

    @Override
    public Filter termsFilter(List values, @Nullable QueryParseContext context) {
        if (context == null) {
            return super.termsFilter(values, context);
        }
        // This will not be invoked if values is empty, so don't check for empty
        if (values.size() == 1) {
            return termFilter(values.get(0), context);
        }

        List<String> types = new ArrayList<>(context.mapperService().types().size());
        for (DocumentMapper documentMapper : context.mapperService().docMappers(false)) {
            if (!documentMapper.parentFieldMapper().active()) {
                types.add(documentMapper.type());
            }
        }

        List<BytesRef> bValues = new ArrayList<>(values.size());
        for (Object value : values) {
            BytesRef bValue = BytesRefs.toBytesRef(value);
            if (Uid.hasDelimiter(bValue)) {
                bValues.add(bValue);
            } else {
                // we use all non child types, cause we don't know if its exact or not...
                for (String type : types) {
                    bValues.add(Uid.createUidAsBytes(type, bValue));
                }
            }
        }
        return new TermsFilter(names.indexName(), bValues);
    }

    /**
     * We don't need to analyzer the text, and we need to convert it to UID...
     */
    @Override
    public boolean useTermQueryWithQueryString() {
        return true;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (!active()) {
            return builder;
        }
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        builder.startObject(CONTENT_TYPE);
        builder.field("type", type);
        builder.field(TypeParsers.DOC_VALUES, storeDocValues);
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        ParentFieldMapper other = (ParentFieldMapper) mergeWith;
        if (active() == other.active()) {
            return;
        }

        if (active() != other.active() || !type.equals(other.type) || hasDocValues() != other.hasDocValues()) {
            mergeContext.addConflict("The _parent field can't be added or updated");
        }
    }

    /**
     * @return Whether the _parent field is actually used.
     */
    public boolean active() {
        return type != null;
    }

    @Override
    public void beforeCreate(DocumentMapper mapper) {
        ParentFieldMapper fieldMapper = mapper.parentFieldMapper();
        if (fieldMapper.active()) {
            ImmutableOpenSet.Builder<String> builder = ImmutableOpenSet.builder(parentTypes);
            builder.add(fieldMapper.type());
            this.parentTypes = builder.build();
        }
    }

    @Override
    public void afterRemove(DocumentMapper mapper) {
        ParentFieldMapper fieldMapper = mapper.parentFieldMapper();
        if (fieldMapper.active()) {
            ImmutableOpenSet.Builder<String> builder = ImmutableOpenSet.builder(parentTypes);
            builder.remove(fieldMapper.type());
            this.parentTypes = builder.build();
        }
    }

    public void setMappingService(MapperService mapperService) {
        ImmutableOpenSet.Builder<String> parentTypesBuilder = ImmutableOpenSet.builder();
        for (DocumentMapper documentMapper : mapperService.docMappers(false)) {
            if (documentMapper.parentFieldMapper().active()) {
                parentTypesBuilder.add(documentMapper.parentFieldMapper().type());
            }
        }
        mapperService.addTypeListener(this);
        this.mapperService = mapperService;
        this.parentTypes = parentTypesBuilder.build();
    }
}
