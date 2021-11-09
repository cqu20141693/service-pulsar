package com.gow.pulsar.core.manager;

import java.util.Set;
import org.apache.pulsar.common.policies.data.AuthAction;

/**
 * @author gow
 * @date 2021/7/8
 */
public interface PulsarManager {

    boolean createTenant(String tenant, Set<String> adminRoles, Set<String> allowedClusters);

    boolean createNamespace(String namespace);
    boolean grantPermissionOnNamespace(String namespace, String role, Set<AuthAction> authActions);

    boolean createPartitionedTopic(String topicName, int nums);

    boolean createNonPartitionedTopic(String topicName);

    boolean grantPermissionOnTopic(String topic, String role, Set<AuthAction> authActions);

    boolean createTokenOnRole(String role);
}
