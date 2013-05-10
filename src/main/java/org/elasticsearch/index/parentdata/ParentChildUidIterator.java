package org.elasticsearch.index.parentdata;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.Set;

/**
 */
// Why isn't there a TermsEnum that combines the terms and docs from two fields?
// Maybe nice to refactor this in a TermsEnum
public class ParentChildUidIterator {

    private final String uidField;
    private final String parentUidField;
    private final Set<String> parentTypes;

    public ParentChildUidIterator(Set<String> parentTypes) {
        this.uidField = UidFieldMapper.NAME;
        this.parentUidField = ParentFieldMapper.NAME;
        this.parentTypes = parentTypes;
    }


    public void iterate(AtomicReader reader, Callback callback) throws IOException {
//        System.out.println("=======================================");
//        System.out.println("reader=" + reader.getCoreCacheKey() + "");
//        System.out.println("=======================================");
        Terms uidTerms = reader.terms(uidField);
        Terms parentUidTerms = reader.terms(parentUidField);
        if (uidTerms == null && parentUidTerms == null) {
            return;
        }

        DocsEnum parentDocsEnum = null;
        DocsEnum childDocsEnum = null;
        if (parentUidTerms != null && uidTerms == null) {
            TermsEnum parentUidTermsEnum = parentUidTerms.iterator(null);
            for (BytesRef parentUidAndType = parentUidTermsEnum.next(); parentUidAndType != null; parentUidAndType = parentUidTermsEnum.next()) {
                BytesRef[] typeAndUid = Uid.splitUidIntoTypeAndId_br(parentUidAndType);
                String type = typeAndUid[0].utf8ToString();
                BytesRef uid = typeAndUid[1];
                childDocsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), childDocsEnum, DocsEnum.FLAG_NONE);
                callback.onUid(type, uid, null, childDocsEnum);
            }
        } else if (parentUidTerms == null) {
            TermsEnum uidTermsEnum = uidTerms.iterator(null);
            for (BytesRef uidAndType = uidTermsEnum.next(); uidAndType != null; uidAndType = uidTermsEnum.next()) {
                BytesRef[] typeAndUid = Uid.splitUidIntoTypeAndId_br(uidAndType);
                String type = typeAndUid[0].utf8ToString();
                BytesRef uid = typeAndUid[1];
                if (parentTypes.contains(type)) {
                    parentDocsEnum = uidTermsEnum.docs(reader.getLiveDocs(), parentDocsEnum, DocsEnum.FLAG_NONE);
                    callback.onUid(type, uid, parentDocsEnum, null);
                }
            }
        } else {
            boolean uidExhausted = false;
            boolean parentUidExhausted = false;
            TermsEnum uidTermsEnum = uidTerms.iterator(null);
            TermsEnum parentUidTermsEnum = parentUidTerms.iterator(null);
            while (true) {
                BytesRef uidAndType = uidExhausted ? null : uidTermsEnum.next();
                BytesRef parentUidAndType = parentUidExhausted ? null : parentUidTermsEnum.next();
//                if (uidAndType != null) {
//                    System.out.println("Uid= " + uidAndType.utf8ToString());
//                }
//                if (parentUidAndType != null) {
//                    System.out.println("parentUid= " + parentUidAndType.utf8ToString());
//                }
                if (uidAndType == null && parentUidAndType == null) {
                    break;
                } else if (uidAndType == null) {
                    for (; parentUidAndType != null; parentUidAndType = parentUidTermsEnum.next()) {
                        BytesRef[] typeAndUid = Uid.splitUidIntoTypeAndId_br(parentUidAndType);
                        String type = typeAndUid[0].utf8ToString();
                        BytesRef uid = typeAndUid[1];
                        childDocsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), childDocsEnum, DocsEnum.FLAG_NONE);
//                        System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                        callback.onUid(type, uid, null, childDocsEnum);
                    }
                    break;
                } else if (parentUidAndType == null) {
                    for (; uidAndType != null; uidAndType = uidTermsEnum.next()) {
                        BytesRef[] typeAndUid = Uid.splitUidIntoTypeAndId_br(uidAndType);
                        String type = typeAndUid[0].utf8ToString();
                        BytesRef uid = typeAndUid[1];
                        if (parentTypes.contains(type)) {
                            parentDocsEnum = uidTermsEnum.docs(reader.getLiveDocs(), parentDocsEnum, DocsEnum.FLAG_NONE);
//                            System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                            callback.onUid(type, uid, parentDocsEnum, null);
                        }
                    }
                    break;
                }

                int cmp = uidAndType.compareTo(parentUidAndType);
//                System.out.println("cmp= " + cmp);
                if (cmp < 0) {
                    do {
                        BytesRef[] typeAndUid = Uid.splitUidIntoTypeAndId_br(uidAndType);
                        String type = typeAndUid[0].utf8ToString();
                        BytesRef uid = typeAndUid[1];
                        if (parentTypes.contains(type)) {
                            parentDocsEnum = uidTermsEnum.docs(reader.getLiveDocs(), parentDocsEnum, DocsEnum.FLAG_NONE);
//                            System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                            callback.onUid(type, uid, parentDocsEnum, null);
                        }
                        uidAndType = uidTermsEnum.next();
                        if (uidAndType != null) {
//                            System.out.println("New uid= " + uidAndType.utf8ToString());
//                            System.out.println("parentUid= " + parentUidAndType.utf8ToString());
                            cmp = uidAndType.compareTo(parentUidAndType);
//                            System.out.println("cmp= " + cmp);
                            // Now the uid and parent field are on the same term
                            if (cmp == 0) {
                                typeAndUid = Uid.splitUidIntoTypeAndId_br(uidAndType);
                                type = typeAndUid[0].utf8ToString();
                                uid = typeAndUid[1];
                                parentDocsEnum = uidTermsEnum.docs(reader.getLiveDocs(), parentDocsEnum, DocsEnum.FLAG_NONE);
                                childDocsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), childDocsEnum, DocsEnum.FLAG_NONE);
//                                System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                                callback.onUid(type, uid, parentDocsEnum, childDocsEnum);
                            } else if (cmp > 0) {
                                // We're about the continue to the next round, but we shouldn't forget the current parentUid
                                typeAndUid = Uid.splitUidIntoTypeAndId_br(parentUidAndType);
                                type = typeAndUid[0].utf8ToString();
                                uid = typeAndUid[1];
                                childDocsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), childDocsEnum, DocsEnum.FLAG_NONE);
//                                System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                                callback.onUid(type, uid, null, childDocsEnum);

                                // And the uid if applicable
                                typeAndUid = Uid.splitUidIntoTypeAndId_br(uidAndType);
                                type = typeAndUid[0].utf8ToString();
                                uid = typeAndUid[1];
                                if (parentTypes.contains(type)) {
                                    parentDocsEnum = uidTermsEnum.docs(reader.getLiveDocs(), parentDocsEnum, DocsEnum.FLAG_NONE);
//                                    System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                                    callback.onUid(type, uid, parentDocsEnum, null);
                                }
                            } else if (cmp < 0) {
                                typeAndUid = Uid.splitUidIntoTypeAndId_br(uidAndType);
                                type = typeAndUid[0].utf8ToString();
                                uid = typeAndUid[1];
                                if (parentTypes.contains(type)) {
                                    parentDocsEnum = uidTermsEnum.docs(reader.getLiveDocs(), parentDocsEnum, DocsEnum.FLAG_NONE);
//                                    System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                                    callback.onUid(type, uid, parentDocsEnum, null);
                                }
                            }
                        } else {
                            // We're about the continue to the next round, but we shouldn't forget the current parentUid
                            typeAndUid = Uid.splitUidIntoTypeAndId_br(parentUidAndType);
                            type = typeAndUid[0].utf8ToString();
                            uid = typeAndUid[1];
                            childDocsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), childDocsEnum, DocsEnum.FLAG_NONE);
//                            System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                            callback.onUid(type, uid, null, childDocsEnum);
                            uidExhausted = true;
                            break;
                        }
                    } while (cmp < 0);
                } else if (cmp == 0) {
                    BytesRef[] typeAndUid = Uid.splitUidIntoTypeAndId_br(uidAndType);
                    String type = typeAndUid[0].utf8ToString();
                    BytesRef uid = typeAndUid[1];
                    parentDocsEnum = uidTermsEnum.docs(reader.getLiveDocs(), parentDocsEnum, DocsEnum.FLAG_NONE);
                    childDocsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), childDocsEnum, DocsEnum.FLAG_NONE);
//                    System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                    callback.onUid(type, uid, parentDocsEnum, childDocsEnum);
                } else if (cmp > 0) {
                    do {
                        BytesRef[] typeAndUid = Uid.splitUidIntoTypeAndId_br(parentUidAndType);
                        String type = typeAndUid[0].utf8ToString();
                        BytesRef uid = typeAndUid[1];
                        childDocsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), childDocsEnum, DocsEnum.FLAG_NONE);
//                        System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                        callback.onUid(type, uid, null, childDocsEnum);

                        parentUidAndType = parentUidTermsEnum.next();
                        if (parentUidAndType != null) {
//                            System.out.println("uid= " + uidAndType.utf8ToString());
//                            System.out.println("new parentUid= " + parentUidAndType.utf8ToString());
                            cmp = uidAndType.compareTo(parentUidAndType);
//                            System.out.println("cmp= " + cmp);
                            // Now the uid and parent field are on the same term
                            if (cmp == 0) {
                                typeAndUid = Uid.splitUidIntoTypeAndId_br(parentUidAndType);
                                type = typeAndUid[0].utf8ToString();
                                uid = typeAndUid[1];
                                parentDocsEnum = uidTermsEnum.docs(reader.getLiveDocs(), parentDocsEnum, DocsEnum.FLAG_NONE);
                                childDocsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), childDocsEnum, DocsEnum.FLAG_NONE);
//                                System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                                callback.onUid(type, uid, parentDocsEnum, childDocsEnum);
                            } else if (cmp < 0) {
                                // We're about the continue to the next round, but we shouldn't forget the current uid
                                typeAndUid = Uid.splitUidIntoTypeAndId_br(uidAndType);
                                type = typeAndUid[0].utf8ToString();
                                uid = typeAndUid[1];
                                if (parentTypes.contains(type)) {
                                    parentDocsEnum = uidTermsEnum.docs(reader.getLiveDocs(), parentDocsEnum, DocsEnum.FLAG_NONE);
//                                    System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                                    callback.onUid(type, uid, parentDocsEnum, null);
                                }

                                typeAndUid = Uid.splitUidIntoTypeAndId_br(parentUidAndType);
                                type = typeAndUid[0].utf8ToString();
                                uid = typeAndUid[1];
                                childDocsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), childDocsEnum, DocsEnum.FLAG_NONE);
//                                System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                                callback.onUid(type, uid, null, childDocsEnum);
                            } else if (cmp > 0) {
                                typeAndUid = Uid.splitUidIntoTypeAndId_br(parentUidAndType);
                                type = typeAndUid[0].utf8ToString();
                                uid = typeAndUid[1];
                                childDocsEnum = parentUidTermsEnum.docs(reader.getLiveDocs(), childDocsEnum, DocsEnum.FLAG_NONE);
//                                System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                                callback.onUid(type, uid, null, childDocsEnum);
                            }
                        } else {
                            // We're about the continue to the next round, but we shouldn't forget the current uid
                            typeAndUid = Uid.splitUidIntoTypeAndId_br(uidAndType);
                            type = typeAndUid[0].utf8ToString();
                            uid = typeAndUid[1];
                            parentDocsEnum = uidTermsEnum.docs(reader.getLiveDocs(), parentDocsEnum, DocsEnum.FLAG_NONE);
//                            System.out.printf("Emit type[%s] and uid[%s]\n", type, uid.utf8ToString());
                            callback.onUid(type, uid, parentDocsEnum, null);
                            parentUidExhausted = true;
                            break;
                        }
                    } while (cmp > 0);
                }
            }
        }
    }


    public interface Callback {

        void onUid(String type, BytesRef uid, DocsEnum parentDocs, DocsEnum childDocs) throws IOException;

    }

}
