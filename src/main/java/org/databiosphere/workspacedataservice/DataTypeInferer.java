package org.databiosphere.workspacedataservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.dao.SingleTenantDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.EntityUpsert;
import org.databiosphere.workspacedataservice.shared.model.UpsertOperation;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.*;
import static org.databiosphere.workspacedataservice.shared.model.UpsertAction.AddUpdateAttribute;
import static org.databiosphere.workspacedataservice.shared.model.UpsertAction.RemoveAttribute;

public class DataTypeInferer {

    private ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, LinkedHashMap<String, DataTypeMapping>> inferTypes(List<EntityUpsert> toUpdate){
        Map<String, LinkedHashMap<String, DataTypeMapping>> result = new HashMap<>();
        for (EntityUpsert entityUpsert : toUpdate) {
            String entityType = entityUpsert.getEntityType();
            List<UpsertOperation> upserts = entityUpsert.getOperations().stream().filter(o -> o.getOp() == AddUpdateAttribute).toList();
            for (UpsertOperation upsert : upserts) {
                result.putIfAbsent(entityType, new LinkedHashMap<>());
                DataTypeMapping typeMapping = inferType(upsert.getAddUpdateAttribute());
                Map<String, DataTypeMapping> attributeMappingForType = result.get(entityType);
                String attributeName = upsert.getAttributeName();
                DataTypeMapping existingTypeMapping = attributeMappingForType.get(attributeName);
                if(existingTypeMapping == null){
                    attributeMappingForType.put(attributeName, typeMapping);
                } else {
                    attributeMappingForType.put(attributeName, selectBestType(existingTypeMapping, typeMapping));
                }
            }
            List<UpsertOperation> removeAttrs = entityUpsert.getOperations().stream().filter(o -> o.getOp() == RemoveAttribute).toList();
            for (UpsertOperation removeAttr : removeAttrs) {
                result.putIfAbsent(entityType, new LinkedHashMap<>());
                Map<String, DataTypeMapping> attributeMappingForType = result.get(entityType);
                //we need to have the attribute being removed present in the schema or else we won't generate
                //the proper update
                attributeMappingForType.put(removeAttr.getAttributeName(), FOR_ATTRIBUTE_DEL);
            }
        }
        return result;
    }

    public DataTypeMapping selectBestType(DataTypeMapping existing, DataTypeMapping newMapping){
        if(existing == newMapping){
            return existing;
        }
        if(existing == LONG && newMapping == DOUBLE){
            return DOUBLE;
        }
        if(existing == DOUBLE && newMapping == LONG){
            return DOUBLE;
        }
        return STRING;
    }


    public DataTypeMapping inferType(Object val){
        if(val instanceof Long || val instanceof Integer){
            return LONG;
        }

        if(val instanceof Double || val instanceof Float){
            return DOUBLE;
        }

        String sVal = val.toString();
        if(isValidDate(sVal)){
            return DATE;
        }
        if(isValidDateTime(sVal)){
            return DATE_TIME;
        }
        if(isValidJson(sVal)){
            return JSON;
        }
        return STRING;
    }

    private boolean isValidJson(String val) {
        try {
            objectMapper.readTree(val);
        } catch (JsonProcessingException e) {
            return false;
        }
        return true;
    }

    private boolean isValidDateTime(String val) {
        try {
            LocalDate.parse(val, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }

    public boolean isValidDate(String val){
        try {
            LocalDate.parse(val, DateTimeFormatter.ISO_LOCAL_DATE);
        } catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }
}
