package org.elasticsearch.test.integration.search.child;

/**
 */
public class ConcreteChildQuerySearchTests extends SimpleChildQuerySearchTests {

    @Override
    protected String getIdCacheType() {
        return "concrete";
    }
}
