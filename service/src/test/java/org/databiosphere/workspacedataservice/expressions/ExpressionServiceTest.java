package org.databiosphere.workspacedataservice.expressions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.web.server.ResponseStatusException;

@SpringBootTest
public class ExpressionServiceTest extends TestBase {
  @Autowired private ExpressionService expressionService;
  @Autowired private RecordDao recordDao;
  @Autowired CollectionService collectionService;
  @Autowired TwdsProperties twdsProperties;
  @Autowired NamedParameterJdbcTemplate namedTemplate;

  UUID collectionUuid;

  @BeforeEach
  void setUp() {
    collectionUuid = collectionService.save(twdsProperties.workspaceId(), "name", "desc").getId();
  }

  @AfterEach
  void tearDown() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
  }

  @Test
  void testExtractRecordAttributeLookups() {
    var result =
        expressionService.extractRecordAttributeLookups(
            """
                { "foo" : this.foo.attribute, "bar" : this.foo.attribute2, "baz" : this.attribute }""");

    assertThat(result)
        .hasSameElementsAs(
            List.of(
                new AttributeLookup(List.of("foo"), "attribute", "this.foo.attribute"),
                new AttributeLookup(List.of("foo"), "attribute2", "this.foo.attribute2"),
                new AttributeLookup(List.of(), "attribute", "this.attribute")));
  }

  /**
   * Tests that given the relations parentType -> childType -> grandChildType,
   * determineExpressionQueries will return the correct ExpressionQueryInfo objects for the
   * attribute lookups from expressions this.pk, this.parentAttr, this.sib1.grand.grandChildAttr,
   * this.sib1.grand.pk, this.sib2.grand.grandChildAttr, this.sib1.childAttr.
   */
  @Test
  void testDetermineExpressionQueries() {
    var grandChildType = RecordType.valueOf("grandChildType");
    var childType = RecordType.valueOf("childType");
    var parentType = RecordType.valueOf("parentType");

    var grandRelation = new Relation("grand", grandChildType);
    var sib1Relation = new Relation("sib1", childType);
    var sib2Relation = new Relation("sib2", childType);

    var pkAttr = "pk";
    var parentAttr = "parentAttr";
    var childAttr = "childAttr";
    var grandChildAttr = "grandChildAttr";

    recordDao.createRecordType(
        collectionUuid,
        Map.of(grandChildAttr, DataTypeMapping.STRING, pkAttr, DataTypeMapping.STRING),
        grandChildType,
        new RelationCollection(Set.of(), Set.of()),
        pkAttr);
    recordDao.createRecordType(
        collectionUuid,
        Map.of(
            childAttr,
            DataTypeMapping.STRING,
            pkAttr,
            DataTypeMapping.STRING,
            grandRelation.relationColName(),
            DataTypeMapping.RELATION),
        childType,
        new RelationCollection(Set.of(grandRelation), Set.of()),
        pkAttr);
    recordDao.createRecordType(
        collectionUuid,
        Map.of(
            parentAttr,
            DataTypeMapping.STRING,
            pkAttr,
            DataTypeMapping.STRING,
            sib1Relation.relationColName(),
            DataTypeMapping.RELATION,
            sib2Relation.relationColName(),
            DataTypeMapping.RELATION),
        parentType,
        new RelationCollection(Set.of(sib1Relation, sib2Relation), Set.of()),
        pkAttr);

    var parentAttrLookup = new AttributeLookup(List.of(), parentAttr, "this.parentAttr");
    var parentPkLookup = new AttributeLookup(List.of(), pkAttr, "this.pk");
    var sib1GrandChildAttrLookup =
        new AttributeLookup(
            List.of(sib1Relation.relationColName(), grandRelation.relationColName()),
            grandChildAttr,
            "this.sib1.grand.grandChildAttr");
    var sib1GrandChildPkLookup =
        new AttributeLookup(
            List.of(sib1Relation.relationColName(), grandRelation.relationColName()),
            pkAttr,
            "this.sib1.grand.pk");
    var sib2GrandChildAttrLookup =
        new AttributeLookup(
            List.of(sib2Relation.relationColName(), grandRelation.relationColName()),
            grandChildAttr,
            "this.sib2.grand.grandChildAttr");
    var sib1ChildAttrLookup =
        new AttributeLookup(
            List.of(sib1Relation.relationColName()), childAttr, "this.sib1.childAttr");
    var results =
        expressionService
            .determineExpressionQueries(
                collectionUuid,
                parentType,
                Set.of(
                    parentAttrLookup,
                    parentPkLookup,
                    sib1GrandChildAttrLookup,
                    sib1GrandChildPkLookup,
                    sib2GrandChildAttrLookup,
                    sib1ChildAttrLookup),
                0)
            .toList();

    // there are 6 lookups which are reduced to 4 queries
    // 1 on the root (no relations) with 2 attributes, 1 on sib1 with 1 attribute,
    // 1 on sib1 -> grand with 2 attributes, 1 on sib2 -> grand with 1 attribute
    // note that there is no sib2 query because there are no lookups with sib2 attributes
    assertThat(results)
        .hasSameElementsAs(
            List.of(
                new ExpressionQueryInfo(List.of(), Set.of(parentAttrLookup, parentPkLookup)),
                new ExpressionQueryInfo(List.of(sib1Relation), Set.of(sib1ChildAttrLookup)),
                new ExpressionQueryInfo(
                    List.of(sib1Relation, grandRelation),
                    Set.of(sib1GrandChildAttrLookup, sib1GrandChildPkLookup)),
                new ExpressionQueryInfo(
                    List.of(sib2Relation, grandRelation), Set.of(sib2GrandChildAttrLookup))));
  }

  @Test
  void testDetermineExpressionQueriesMissingRelation() {
    var recordType = RecordType.valueOf("recordType");

    var pkAttr = "pk";

    recordDao.createRecordType(
        collectionUuid,
        Map.of(pkAttr, DataTypeMapping.STRING),
        recordType,
        new RelationCollection(Set.of(), Set.of()),
        pkAttr);

    var missingRelation = "missingRelation";
    assertThatThrownBy(
            () ->
                expressionService.determineExpressionQueries(
                    collectionUuid,
                    recordType,
                    Set.of(
                        new AttributeLookup(
                            List.of(missingRelation),
                            "missingAttr",
                            "this.missingRelation.missingAttr")),
                    0))
        .isInstanceOfSatisfying(
            ResponseStatusException.class,
            e -> {
              assertThat(e.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
              assertThat(e.getReason()).contains(missingRelation);
            });
  }
}
