package com.gow.pulsar.core.manager.token;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author gow
 * @date 2021/7/14
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "jwt.broker")
public class PulsarJwtConfig {

    private Integer sessionTime;

    private String tokenMode;

    private String secretKey;

    private String privateKey;

    private String publicKey;
}
