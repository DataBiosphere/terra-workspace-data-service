package org.databiosphere.workspacedataservice;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;

public class Main {
    public static void main(String[] args) throws JsonProcessingException {
        var mapper = new ObjectMapper().configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        var test = new Test(new BigDecimal("0.0000000005"));
        System.out.println(mapper.writeValueAsString(test));
    }
}

class Test {
    private final BigDecimal value;
    Test(BigDecimal value) {
        this.value = value;
    }

    @JsonFormat(shape= JsonFormat.Shape.STRING)
    public BigDecimal getValue() {
        return value;
    }
}
