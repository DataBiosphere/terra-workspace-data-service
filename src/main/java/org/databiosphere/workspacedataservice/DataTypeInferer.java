package org.databiosphere.workspacedataservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.EntityUpsert;
import org.databiosphere.workspacedataservice.shared.model.UpsertOperation;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.*;
import static org.databiosphere.workspacedataservice.shared.model.UpsertAction.AddUpdateAttribute;

public class DataTypeInferer {

    private ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, Map<String, DataTypeMapping>> inferTypes(List<EntityUpsert> toUpdate){
        Map<String, Map<String, DataTypeMapping>> result = new HashMap<>();
        for (EntityUpsert entityUpsert : toUpdate) {
            String entityType = entityUpsert.getEntityType();
            List<UpsertOperation> upserts = entityUpsert.getOperations().stream().filter(o -> o.getOp() == AddUpdateAttribute).toList();
            for (UpsertOperation upsert : upserts) {
                result.putIfAbsent(entityType, new HashMap<>());
                DataTypeMapping typeMapping = inferType(upsert.getAddUpdateAttribute().toString());
                Map<String, DataTypeMapping> attributeMappingForType = result.get(entityType);
                String attributeName = upsert.getAttributeName();
                DataTypeMapping existingTypeMapping = attributeMappingForType.get(attributeName);
                if(existingTypeMapping == null){
                    attributeMappingForType.put(attributeName, typeMapping);
                } else {
                    attributeMappingForType.put(attributeName, selectBestType(existingTypeMapping, typeMapping));
                }
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
        return STRING;
    }


    public DataTypeMapping inferType(String val){
        if(Ints.tryParse(val) != null){
            return LONG;
        }
        if(Doubles.tryParse(val) != null){
            return DOUBLE;
        }
        if(isValidDate(val)){
            return DATE;
        }
        if(isValidDateTime(val)){
            return DATE_TIME;
        }
        if(isValidJson(val)){
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
