package org.example.emq.client;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.example.emq.config.MqttClientConfig;
import org.example.emq.utils.SslContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author 罗涛
 * @title MqttAsyncClient
 * @date 2022/4/24 11:14
 */
@Slf4j
@Component
public class EventMqttClient implements SmartLifecycle {
    private static ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private static final MemoryPersistence persistence = new MemoryPersistence();

    @Autowired
    MqttClientConfig mqttClientConfig;

    public static MqttConnectOptions mqttConnectOptions;
    private IMqttAsyncClient mqttAsyncClient;
    private MqttClientHandler clientHandler;

    private boolean running = false;
    private int retry = 0;

    @Override
    public void start() {
        mqttConnectOptions = buildCommonOptions();
        buildAsyncClient();
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        try {
            if (Objects.isNull(mqttAsyncClient) || !mqttAsyncClient.isConnected()) {
                return;
            }
            mqttAsyncClient.close();
        } catch (Exception e) {
            log.error("发送Mqtt消息发生异常：" + e.getMessage(), e);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }


    private void buildAsyncClient(){
        try {
            String hostUrl = buildDefaultUrl();
            String clientId = buildClientId();
            MqttAsyncClient eventMqttClient = new MqttAsyncClient(hostUrl, clientId, persistence);
            if (Objects.isNull(clientHandler)) {
                MqttClientHandler callbackHandler = new MqttClientHandler();
                this.clientHandler = callbackHandler;
            }
            this.mqttAsyncClient = eventMqttClient;
            this.mqttAsyncClient.setCallback(clientHandler);
            eventMqttClient.connect(mqttConnectOptions, hostUrl, clientHandler);
        } catch (MqttException e) {
            log.error("创建mqtt client发生异常：{}", e.getMessage(), e);
            scheduledExecutorService.schedule(this::buildAsyncClient, retryInterval(), TimeUnit.SECONDS);
        }
    }

    /**
     * 生成clientId, 类名+IP
     * @return string
     */
    private String buildClientId() {
        String className = this.getClass().getSimpleName();
//        String localIp = SystemUtil.getRealIP();
        String uuid = UUID.randomUUID().toString();
        String shortUuid = StringUtils.substring(uuid, uuid.length() - 8);
        return StringUtils.joinWith("-", className, shortUuid);
    }

    /**
     * 生成mqtt server连接地址
     * @return
     */
    private String buildDefaultUrl() {
        Boolean useSsl = mqttClientConfig.getSslEnable();
        String host = mqttClientConfig.getHost();
        int port = Objects.nonNull(useSsl) && useSsl ? mqttClientConfig.getSslPort() : mqttClientConfig.getPort();
        String tcpDefaultProtocol = Objects.nonNull(useSsl) && useSsl ? "ssl://" : "tcp://";
        return StringUtils.join(tcpDefaultProtocol, host, ":", port);
    }

    /**
     * mqtt 连接配置
     * @return
     */
    private MqttConnectOptions buildCommonOptions() {
        int timeout = mqttClientConfig.getTimeout();
        int keepalive = mqttClientConfig.getKeepalive();
        MqttConnectOptions options = new MqttConnectOptions();
        // 相同的clientId各个节点同时订阅,通过分布式锁,仅一个节点进行数据处理
        options.setCleanSession(true);
        options.setAutomaticReconnect(false);
        options.setConnectionTimeout(timeout);
        options.setKeepAliveInterval(keepalive);
        String username = mqttClientConfig.getUsername();
        String password = mqttClientConfig.getPassword();
        options.setUserName(username);
        if (StringUtils.isNotEmpty(password)) {
            options.setPassword(password.toCharArray());
        }
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        options.setMaxInflight(1000);
        Boolean sslEnable = mqttClientConfig.getSslEnable();
        if (Objects.nonNull(sslEnable) && sslEnable) {
            options.setSocketFactory(getSocketFactory());
        }
        return options;
    }

    /**
     * 加密连接工厂
     * @return factory
     */
    private SSLSocketFactory getSocketFactory() {
        try {
            String sslPassword = mqttClientConfig.getSslPassword();
            String clientCertPath = mqttClientConfig.getClientCertPath();
            String rootCertPath = mqttClientConfig.getRootCertPath();
            SSLContext clientContext = SslContextUtil.getClientContext(clientCertPath, sslPassword, rootCertPath,
                    sslPassword);
            return clientContext.getSocketFactory();
        } catch (Exception e) {
            log.error("创建mqtt ssl-socket-factory失败： cause = {}", e.getMessage(), e);
        }
        return null;
    }

    public int retryInterval() {
        retry++;
        return Math.min(retry, 15) * 10;
    }



    public void publish(String topic, byte[] pushMessage){
        int defaultQos = 1;
        boolean defaultRetain = false;
        MqttMessage message = new MqttMessage();
        message.setQos(defaultQos);
        message.setRetained(defaultRetain);
        message.setPayload(pushMessage);
        try {
            if (Objects.isNull(mqttAsyncClient) || !mqttAsyncClient.isConnected()) {
                return;
            }
            log.info("发布mqtt单点登录事件： topic = {}, payload = {}", topic, new String(pushMessage));
            mqttAsyncClient.publish(topic, message);
        } catch (Exception e) {
            log.error("发送Mqtt消息发生异常：" + e.getMessage(), e);
        }
    }



    private class MqttClientHandler implements MqttCallback, IMqttActionListener {

        @Override
        public void onSuccess(IMqttToken asyncActionToken) {
            Object hostUrl = asyncActionToken.getUserContext();
            String clientId = EventMqttClient.this.buildClientId();
            log.info("mqtt client与服务器({})连接成功：clientId = {}",hostUrl.toString(), clientId);
            retry = 0;
            // 订阅默认主题
            List<String> defaultTopics = mqttClientConfig.getDefaultTopics();
            if (!CollectionUtils.isEmpty(defaultTopics)) {
                log.info("mqtt client订阅默认主题：clientId = {}, topic = {}", clientId, JSON.toJSONString(defaultTopics));
                defaultTopics.forEach(topic -> {
                            try {
                                mqttAsyncClient.subscribe(topic, 1);
                            } catch (MqttException e) {
                                log.error("mqtt client订阅默认主题发生异常：clientId = {}, topic = {}, cause = {}", clientId, topic, e.getMessage(), e);
                                return;
                            }
                        }
                );
            }
        }

        @Override
        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
            if (EventMqttClient.this.isRunning()) {
                int delay = retryInterval();
                String clientId = EventMqttClient.this.buildClientId();
                log.warn("连接MQTT Server失败, {}秒后发起重连: clientId = {}", delay, clientId);
                scheduledExecutorService.schedule(EventMqttClient.this::buildAsyncClient, delay, TimeUnit.SECONDS);
            }
        }

        @Override
        public void connectionLost(Throwable cause) {
            if (EventMqttClient.this.isRunning()) {
                int delay = retryInterval();
                String clientId = EventMqttClient.this.buildClientId();
                log.error("与mqtt server断开连接，{}秒后发起重连：clientId = {}, cause = {}", delay, clientId, cause.getMessage(), cause);
                scheduledExecutorService.schedule(EventMqttClient.this::buildAsyncClient, delay, TimeUnit.SECONDS);
            }
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
            String clientId = mqttAsyncClient.getClientId();
            try {
                byte[] payload = message.getPayload();
                String payloadData = new String(payload);
                log.info("接收消息主题 : " + topic);
                log.info("接收消息Qos : " + message.getQos());
                log.info("接收消息内容 : " + payloadData);
            } catch (Exception ex) {
                log.error("处理mqtt消息时发生异常， topic = {}, client = {}", topic, clientId);
                throw ex;
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            log.info("deliveryComplete");
        }
    }
}
