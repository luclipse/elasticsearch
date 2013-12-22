package org.elasticsearch.index.parentordinals;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.warmer.IndicesWarmer;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 */
public final class ParentOrdinalsService extends AbstractIndexShardComponent {

    private final MapperService mapperService;
    private final ParentOrdinals.Builder builder;

    private volatile ParentOrdinals current;

    @Inject
    public ParentOrdinalsService(ShardId shardId, @IndexSettings Settings indexSettings, MapperService mapperService, ParentOrdinals.Builder builder) {
        super(shardId, indexSettings);
        this.mapperService = mapperService;
        this.builder = builder;
    }

    @SuppressWarnings("unchecked")
    public void refresh(IndicesWarmer.WarmerContext context) throws IOException {
        // We don't want to load uid of child documents, this allows us to not load uids of child types.
        NavigableSet<BytesRef> parentTypes = new TreeSet<BytesRef>(BytesRef.getUTF8SortedAsUnicodeComparator());
        for (String type : mapperService.types()) {
            ParentFieldMapper parentFieldMapper = mapperService.documentMapper(type).parentFieldMapper();
            if (parentFieldMapper.active()) {
                parentTypes.add(new BytesRef(parentFieldMapper.type()));
            }
        }
        current = builder.build(current, context, parentTypes);
    }

    public ParentOrdinals current() {
        assert current != null;
        return current;
    }

}
