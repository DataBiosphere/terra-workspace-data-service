package org.databiosphere.workspacedataservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.dao.SingleTenantDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.EntityUpsert;
import org.databiosphere.workspacedataservice.shared.model.UpsertOperation;
import org.postgresql.shaded.com.ongres.scram.common.bouncycastle.pbkdf2.Integers;

import java.time.LocalDate;
import java.time.LocalDateTime;
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

    public Object convertToType(Object val, DataTypeMapping typeMapping) {
        return switch (typeMapping){
            case DATE -> LocalDate.parse(val.toString(), DateTimeFormatter.ISO_LOCAL_DATE);
            case DATE_TIME -> LocalDateTime.parse(val.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            case LONG, DOUBLE, STRING, JSON, BOOLEAN -> val;
            case FOR_ATTRIBUTE_DEL -> throw new IllegalArgumentException("Egad, this should not happen");
        };
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


    /**
     * Order matters; we want to choose the most specific type.  "1234" is valid json, but the code
     * chooses to infer it as a LONG (bigint in the db).  "true" is a string and valid json but the
     * code is ordered to infer boolean. true is also valid json but we want to infer boolean.
     * @param val
     * @return
     */
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
        if(StringUtils.isNumeric(sVal)){
            if(Longs.tryParse(sVal) != null){
                return LONG;
            }
            return DOUBLE;
        }
        if(sVal.equalsIgnoreCase("true") || sVal.equalsIgnoreCase("false")){
            return BOOLEAN;
        }
        if(isValidJson(sVal)){
            return JSON;
        }
        return STRING;
    }

    protected boolean isValidJson(String val) {
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
