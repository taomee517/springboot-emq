package org.example.emq.client;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.example.emq.config.MqttConfig;
import org.example.emq.handler.PushCallback;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * @author 罗涛
 * @title MqttPushClient
 * @date 2020/10/10 17:38
 */
@Getter
@Slf4j
@Component
public class MqttPushClient implements SmartLifecycle {

    @Autowired
    private PushCallback pushCallback;

    @Autowired
    MqttConfig mqttConfig;

    private MqttClient client;
    private boolean running;
    private MemoryPersistence memoryPersistence;


    @Override
    public void start() {
        try {
            String hostUrl = mqttConfig.getHostUrl();
            String clientId = mqttConfig.getClientId();
            String username = mqttConfig.getUsername();
            String password = mqttConfig.getPassword();
            int timeout = mqttConfig.getTimeout();
            int keepalive = mqttConfig.getKeepalive();
            memoryPersistence = new MemoryPersistence();
            client = connect(hostUrl, clientId, username, password, timeout, keepalive, memoryPersistence);
            log.info("与mqttServer连接建立成功:{}", client.isConnected());
            // 以/#结尾表示订阅所有以test开头的主题
            List<String> defaultTopics = mqttConfig.getDefaultTopics();
            if(!CollectionUtils.isEmpty(defaultTopics)){
                String [] topicArray = defaultTopics.toArray(new String[defaultTopics.size()]);
                log.info("订阅默认主题:{}", defaultTopics.toString());
                client.subscribe(topicArray);
            }
            running = true;
        } catch (Exception e) {
            log.error("初始化MqttClient时发生异常：" + e.getMessage(), e);
            start();
        }
    }

    @Override
    public void stop() {
        try {
            if (client.isConnected()) {
                memoryPersistence.close();
                client.disconnect();
                client.close();
            }
            running = false;
        } catch (MqttException e) {
            log.error("MqttClient关闭时发生异常：" + e.getMessage(), e);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * 客户端连接
     *
     * @param host      ip+端口
     * @param clientID  客户端Id
     * @param username  用户名
     * @param password  密码
     * @param timeout   超时时间
     * @param keepalive 保留数
     */
    public MqttClient connect(String host, String clientID, String username, String password,
                              int timeout, int keepalive, MemoryPersistence memoryPersistence) throws Exception {
        if(Objects.isNull(memoryPersistence) || StringUtils.isEmpty(clientID)) return null;
        MqttClient client = new MqttClient(host, clientID, memoryPersistence);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setUserName(username);
        options.setPassword(password.toCharArray());
        options.setConnectionTimeout(timeout);
        options.setKeepAliveInterval(keepalive);
        client.setCallback(pushCallback);
        client.connect(options);
        return client;
    }

    /**
     * 发布
     *
     * @param qos         连接方式
     * @param retained    是否保留
     * @param topic       主题
     * @param pushMessage 消息体
     */
    public void publish(int qos, boolean retained, String topic, String pushMessage) {
        MqttMessage message = new MqttMessage();
        message.setQos(qos);
        message.setRetained(retained);
        message.setPayload(pushMessage.getBytes());
        MqttTopic mTopic = client.getTopic(topic);
        if (null == mTopic) {
            log.error("topic not exist");
        }
        MqttDeliveryToken token;
        try {
            token = mTopic.publish(message);
            token.waitForCompletion();
        } catch (MqttPersistenceException e) {
            e.printStackTrace();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}


