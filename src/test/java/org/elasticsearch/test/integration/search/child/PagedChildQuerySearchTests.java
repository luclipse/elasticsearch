package org.elasticsearch.test.integration.search.child;

/**
 */
public class PagedChildQuerySearchTests extends SimpleChildQuerySearchTests {

    @Override
    protected String getIdCacheType() {
        return "paged";
    }
}
