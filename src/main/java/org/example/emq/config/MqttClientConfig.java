/*
 * Copyright [2020] [MaxKey of copyright http://www.maxkey.top]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 

package org.example.emq.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties("config.mqtt.client")
public class MqttClientConfig {
    /**
     * 用户名
     */
    private String username;
    /**
     * 密码
     */
    private String password;

    /**
     * 是否采用加密传输
     */
    private Boolean sslEnable;


    /**
     * mqtt server的ip或域名
     */
    private String host;

    /**
     * 明文数据传输端口
     */
    private Integer port;

    /**
     * 密文数据传输端口
     */
    private Integer sslPort;

    /**
     * 超时时间(s)
     */
    private Integer timeout;
    /**
     * 保活间隔时间
     */
    private Integer keepalive;

    /**
     * SSL密钥文件密码
     */
    private String sslPassword;

    /**
     * 客户端私钥路径
     */
    private String clientKeyPath;

    /**
     * 证书路径
     */
    private String clientCertPath;
    private String rootCertPath;

    /**
     * 默认订阅主题
     * @return
     */
    private List<String> defaultTopics;

}
