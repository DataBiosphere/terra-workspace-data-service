package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.*;

public class DataTypeInferer {

    private ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, DataTypeMapping> inferTypes(Map<String, Object> updatedAtts) {
        Map<String, DataTypeMapping> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : updatedAtts.entrySet()) {
            result.put(entry.getKey(), inferType(entry.getValue()));
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
