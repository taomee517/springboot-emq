package org.example.emq.client;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.example.emq.config.MqttClientConfig;
import org.example.emq.utils.SnUtil;
import org.example.emq.utils.SslContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.util.*;
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
public class EventMqttBatchClient implements SmartLifecycle {
    private static ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private static final MemoryPersistence persistence = new MemoryPersistence();

    @Autowired
    MqttClientConfig mqttClientConfig;

    public static MqttConnectOptions mqttConnectOptions;
    private static Set<IMqttAsyncClient> mqttAsyncClients = new HashSet<>();
    private MqttClientHandler clientHandler;

    private boolean running = false;

    @Override
    public void start() {
        mqttConnectOptions = buildCommonOptions();
        buildClients();
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        try {
            if (CollectionUtils.isEmpty(mqttAsyncClients)) {
                return;
            }
            for (IMqttAsyncClient mqttAsyncClient: mqttAsyncClients) {
                mqttAsyncClient.close();
            }
        } catch (Exception e) {
            log.error("发送Mqtt消息发生异常：" + e.getMessage(), e);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }


    private void buildClients(){
        Random random = new Random();
        for (int i=0; i<10000; i++) {
            try {
                String clientId = buildClientId(i);
                createSingleClient(clientId);
                int sleepTime = random.nextInt(10);
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void createSingleClient(String clientId){
        try {
            String hostUrl = buildDefaultUrl();
            MqttAsyncClient eventMqttClient = new MqttAsyncClient(hostUrl, clientId, persistence);
            if (Objects.isNull(clientHandler)) {
                MqttClientHandler callbackHandler = new MqttClientHandler();
                this.clientHandler = callbackHandler;
            }
            IMqttAsyncClient mqttAsyncClient = eventMqttClient;
            mqttAsyncClient.setCallback(clientHandler);
            eventMqttClient.connect(mqttConnectOptions, null, clientHandler);
        } catch (MqttException e) {
            log.error("创建mqtt client发生异常：{}", e.getMessage(), e);
            scheduledExecutorService.schedule(()->this.createSingleClient(clientId), retryInterval(clientId), TimeUnit.SECONDS);
        }
    }

    /**
     * 生成clientId, 类名+IP
     * @return string
     */
    private String buildClientId(int i) {
        String className = this.getClass().getSimpleName();
        return StringUtils.joinWith("-", className, i);
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


    public int retryInterval(String client) {
        int retry = SnUtil.getSn(client);
        return Math.min(retry, 15) * 10;
    }


    private class MqttClientHandler implements MqttCallback, IMqttActionListener {

        @Override
        public void onSuccess(IMqttToken asyncActionToken) {
            IMqttAsyncClient client = asyncActionToken.getClient();
            String clientId = client.getClientId();
            String hostUrl = client.getServerURI();
            mqttAsyncClients.add(client);
            log.info("mqtt client与服务器({})连接成功：clientId = {}",hostUrl, clientId);
            SnUtil.resetSn(clientId);
            // 订阅默认主题
            List<String> defaultTopics = mqttClientConfig.getDefaultTopics();
            if (!CollectionUtils.isEmpty(defaultTopics)) {
                Collections.shuffle(defaultTopics);
                defaultTopics.stream().findAny().ifPresent(topic -> {
                    log.info("mqtt client订阅默认主题：clientId = {}, topic = {}", clientId, topic);
                    try {
                        client.subscribe(topic, 1);
                    } catch (MqttException e) {
                        log.error("mqtt client订阅默认主题发生异常：clientId = {}, topic = {}, cause = {}", clientId, topic, e.getMessage(), e);
                        return;
                    }
                });
            }
        }

        @Override
        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
            if (EventMqttBatchClient.this.isRunning()) {
                IMqttAsyncClient client = asyncActionToken.getClient();
                String clientId = client.getClientId();
                String hostUrl = client.getServerURI();
                int delay = retryInterval(clientId);
                log.warn("连接MQTT Server失败, {}秒后发起重连: clientId = {}", delay, clientId);
                scheduledExecutorService.schedule(()->EventMqttBatchClient.this.createSingleClient(clientId), delay, TimeUnit.SECONDS);
            }
        }

        @Override
        public void connectionLost(Throwable cause) {
            if (EventMqttBatchClient.this.isRunning()) {
                log.error("与mqtt server断开连接 cause = {}", cause.getMessage(), cause);
//                int delay = retryInterval(clientId);
//                String clientId = EventMqttBatchClient.this.buildClientId();
//                log.error("与mqtt server断开连接，{}秒后发起重连：clientId = {}, cause = {}", delay, "--", cause.getMessage(), cause);
//                scheduledExecutorService.schedule(EventMqttBatchClient.this::buildAsyncClient, delay, TimeUnit.SECONDS);
            }
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
//            String clientId = mqttAsyncClient.getClientId();
            try {
                byte[] payload = message.getPayload();
                String payloadData = new String(payload);
                log.info("接收消息主题 : {}, 内容: {}" + topic, payloadData);
            } catch (Exception ex) {
                log.error("处理mqtt消息时发生异常， topic = {}, client = {}", topic, "--");
                throw ex;
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            log.info("deliveryComplete");
        }
    }
}
