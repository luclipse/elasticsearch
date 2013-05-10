package org.elasticsearch.test.integration.search.child;

/**
 */
public class FSTChildQuerySearchTests extends SimpleChildQuerySearchTests {

    @Override
    protected String getIdCacheType() {
        return "fst";
    }
}
