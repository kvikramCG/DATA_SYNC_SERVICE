package com.datasync.config;

import com.datasync.adapter.out.ledgerpoint.LedgerPointProperties;
import com.datasync.adapter.out.orion.OrionProperties;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CredentialValidationTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void orionWithABlankPasswordIsRejected() {
        OrionProperties properties = validOrion();
        properties.setPassword("");

        assertThat(validator.validate(properties)).extracting(v -> v.getPropertyPath().toString())
                .containsExactly("password");
    }

    @Test
    void orionWithAllRequiredValuesIsAccepted() {
        assertThat(validator.validate(validOrion())).isEmpty();
    }

    @Test
    void ledgerPointWithABlankApiKeyIsRejected() {
        LedgerPointProperties properties = validLedgerPoint();
        properties.setApiKey("  ");

        assertThat(validator.validate(properties)).extracting(v -> v.getPropertyPath().toString())
                .containsExactly("apiKey");
    }

    @Test
    void ledgerPointWithAllRequiredValuesIsAccepted() {
        assertThat(validator.validate(validLedgerPoint())).isEmpty();
    }

    private static OrionProperties validOrion() {
        OrionProperties properties = new OrionProperties();
        properties.setBaseUrl("https://orion.example.com");
        properties.setLogin("svc-data-sync");
        properties.setPassword("secret");
        return properties;
    }

    private static LedgerPointProperties validLedgerPoint() {
        LedgerPointProperties properties = new LedgerPointProperties();
        properties.setBaseUrl("https://ledgerpoint.example.com");
        properties.setApiKey("key");
        return properties;
    }
}
