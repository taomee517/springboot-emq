package org.example.emq.handler;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.example.emq.client.MqttPushClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author 罗涛
 * @title PushCallback
 * @date 2020/10/10 17:39
 */

@Slf4j
@Component
public class PushCallback implements MqttCallback {

    @Autowired
    private MqttPushClient mqttPushClient;

    @Override
    public void connectionLost(Throwable throwable) {
        // 连接丢失后，一般在这里面进行重连
        log.info("连接断开，重连MQTT Server：trowType = {}", throwable.getClass());
        try {
            mqttPushClient.stop();
        } catch (Exception e){
            e.printStackTrace();
        }finally {
            mqttPushClient.start();
        }

    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        // subscribe后得到的消息会执行到这里面
        log.info("接收消息主题 : " + topic);
        log.info("接收消息Qos : " + mqttMessage.getQos());
        log.info("接收消息内容 : " + new String(mqttMessage.getPayload()));
//        int a = 1/0;
        mqttPushClient.publish(mqttMessage.getQos(),false, "$creq/00023ED4", "$00023ED40004FF");
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        log.info("deliveryComplete---------" + iMqttDeliveryToken.isComplete());
    }
}

