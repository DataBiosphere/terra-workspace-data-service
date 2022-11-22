package org.databiosphere.workspacedataservice.shared.model;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordAttributesTest {


    @Test
    void verifyAttributeOrdering() {
        RecordAttributes recordAttributes = RecordAttributes.empty("Z");
        recordAttributes.putAttribute("a", 11);
        recordAttributes.putAttribute("A", 17);
        recordAttributes.putAttribute("B", "hello");
        recordAttributes.putAttribute("C", LocalDate.of(2022, 11, 21));
        recordAttributes.putAttribute("Z", "1");
        List<String> attributeNamesInOrder = recordAttributes.attributeSet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        assertThat(attributeNamesInOrder).isEqualTo(List.of("Z", "a", "A", "B", "C"));
    }
}
