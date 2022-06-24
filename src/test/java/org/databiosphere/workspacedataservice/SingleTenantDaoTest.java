package org.databiosphere.workspacedataservice;

import com.google.common.primitives.Doubles;
import org.databiosphere.workspacedataservice.dao.SingleTenantDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class SingleTenantDaoTest {

    private final SingleTenantDao dao;

    public SingleTenantDaoTest(@Autowired SingleTenantDao dao) {
        this.dao = dao;
    }

    @Test
    void schemaExits(){
        UUID uuid = UUID.randomUUID();
        assertFalse(dao.workspaceSchemaExists(uuid), "Random schema should not exists");
    }

    @Test
    void createSchema(){
        UUID uuid = UUID.randomUUID();
        dao.createSchema(uuid);
        assertTrue(dao.workspaceSchemaExists(uuid), "Schema should have been created");
    }

    @Test
    void tableExists(){
        UUID uuid = UUID.randomUUID();
        assertFalse(dao.entityTypeExists(uuid, "foo"), "entity type should not exist");
    }

    @Test
    void createTable(){
        UUID uuid = UUID.randomUUID();
        dao.createSchema(uuid);
        dao.createEntityType(uuid, Map.of("participant", DataTypeMapping.STRING, "sample", DataTypeMapping.STRING, "date_collected", DataTypeMapping.DATE, "score", DataTypeMapping.LONG), "foo");
    }

    @Test
    void getExistingAttributes(){
        UUID workspaceId = UUID.fromString("53204e0d-2d7a-4c3c-867f-04436e7f3c61");
        Map<String, DataTypeMapping> foo = dao.getExistingTableSchema(workspaceId, "foo");
        dao.changeColumn(workspaceId, "foo", "bar", DataTypeMapping.STRING);
//        dao.addColumn(workspaceId, "foo", "bar", DataTypeMapping.JSON);
    }

    @Test
    void insertEntities(){
        UUID workspaceId = UUID.fromString("53204e0d-2d7a-4c3c-867f-04436e7f3c61");
        Map<String, DataTypeMapping> foo = dao.getExistingTableSchema(workspaceId, "foo");
        Entity e = new Entity("huzzah", new EntityType("foo"), Map.of("date_collected", LocalDate.of(2001, 12, 1), "score", 99001, "participant", "Drew", "sample", "sample1", "bar", "bazz"));
        dao.insertEntities(workspaceId, "foo", Collections.singletonList(e), new LinkedHashMap<>(dao.getExistingTableSchema(workspaceId, "foo")));
//        dao.addColumn(workspaceId, "foo", "bar", DataTypeMapping.JSON);
    }

    @Test
    void updateEntites(){
        UUID workspaceId = UUID.fromString("53204e0d-2d7a-4c3c-867f-04436e7f3c61");
//        dao.addColumn(workspaceId, "foo", "att_list", DataTypeMapping.JSON);
        Map<String, Object> date_collected = Map.of("att_list", "{\"list\": [\"a\", \"b\"]}");
        HashMap<String, Object> toUpdate = new HashMap<>(date_collected);
        toUpdate.put("score", 10000101);
        Entity e = new Entity("huzzah", new EntityType("foo"), toUpdate);
        dao.updateEntities(workspaceId, "foo", Collections.singletonList(e), dao.getExistingTableSchema(workspaceId, "foo"));
//        dao.addColumn(workspaceId, "foo", "bar", DataTypeMapping.JSON);
    }




}
