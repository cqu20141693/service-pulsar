package com.gow.pulsar.core.manager;

import com.alibaba.fastjson.JSONObject;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

/**
 * @author gow
 * @date 2021/7/8
 */
@Component
@Slf4j
@ConditionalOnBean(PulsarAdmin.class)
public class DefaultPulsarManager implements PulsarManager {
    @Autowired
    private PulsarAdmin pulsarAdmin;

    @Override
    public boolean createTenant(String tenant, Set<String> adminRoles, Set<String> allowedClusters) {
        try {

            Tenants tenants = pulsarAdmin.tenants();
            TenantInfo tenantInfo =
                    TenantInfoImpl.builder().adminRoles(adminRoles).allowedClusters(allowedClusters).build();
            tenants.createTenant(tenant, tenantInfo);
            return true;
        } catch (PulsarAdminException pulsarAdminException) {
            log.error("createTenant failed tenant={},roles={},cluster={}", tenant, adminRoles, adminRoles,
                    pulsarAdminException);
        }
        return false;
    }

    @Override
    public boolean createNamespace(String namespace) {
        try {

            Namespaces namespaces = pulsarAdmin.namespaces();
            namespaces.createNamespace(namespace);
            return true;
        } catch (PulsarAdminException pulsarAdminException) {
            log.error("createNamespace failed namespace={}", namespace, pulsarAdminException);
        }

        return false;
    }

    @Override
    public boolean grantPermissionOnNamespace(String namespace, String role, Set<AuthAction> authActions) {
        try {

            Namespaces namespaces = pulsarAdmin.namespaces();
            namespaces.grantPermissionOnNamespace(namespace, role, authActions);
            return true;
        } catch (PulsarAdminException pulsarAdminException) {
            log.error("grantPermissionOnNamespace failed namespace={},role={},authAction={}", namespace, role,
                    JSONObject.toJSONString(authActions), pulsarAdminException);
        }

        return false;
    }

    @Override
    public boolean createPartitionedTopic(String topicName, int nums) {

        try {
            Topics topics = pulsarAdmin.topics();
            topics.createPartitionedTopic(topicName, nums);
            return true;
        } catch (PulsarAdminException pulsarAdminException) {
            log.error("createPartitionedTopic failed topicName={},nums={}", topicName, nums, pulsarAdminException);
            pulsarAdminException.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean createNonPartitionedTopic(String nonTopicName) {
        try {
            Topics topics = pulsarAdmin.topics();
            topics.createNonPartitionedTopic(nonTopicName);
            return true;
        } catch (PulsarAdminException pulsarAdminException) {
            log.error("createNonPartitionedTopic failed  topicName={}", nonTopicName, pulsarAdminException);
        }
        return false;
    }

    public boolean grantPermissionOnTopic(String topic, String role, Set<AuthAction> authActions) {
        try {
            Topics topics = pulsarAdmin.topics();
            topics.grantPermission(topic, role, authActions);
            return true;
        } catch (PulsarAdminException e) {
            log.error("grantPermissionOnTopic failed namespace={},role={},authAction={}", topic, role,
                    JSONObject.toJSONString(authActions), e);
        }
        return false;
    }

    @Override
    public boolean createTokenOnRole(String role) {
        //todo 通过登录pulsar-manager ，然后调用接口进行token 生成
        // uri pulsar-manager/tokens/token
        // token 验证原理，将role放到jwt中，而后通过解析jwt得到role再进行权限认证，
        // pulsar中只保留了role和tenant,namespace,topic的授权关系
        return false;
    }
}
