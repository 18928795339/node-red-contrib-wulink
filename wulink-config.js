const mqtt = require('mqtt');
const crypto = require('crypto');

module.exports = function (RED) {
  class WulinkConfigNode {
    constructor(config) {
      RED.nodes.createNode(this, config);
      const node = this;
      // 初始化配置
      node.authType = config.authType || 'device';
      node.productKey = config.productKey;
      node.productSecret = config.productSecret;
      node.serialNumber = config.serialNumber;
      node.deviceName = config.deviceName;
      node.deviceSecret = config.deviceSecret;
      node.server = config.server || 'iot.wulink.tech';
      node.port = config.port || '8883';

      // 连接状态管理
      node.mqttClient = null;
      node.connectionStatus = 'disconnected';
      node.isClosing = false;
      node.reconnectTimer = null;
      node.reconnectAttempt = 0;
      node.reconnectBaseDelay = 2000;
      node.reconnectMaxDelay = 120000;
      node.connectionTimeout = null;
      node.boundMqttHandlers = null;
      node.mqttUrl = null;
      node.mqttOptions = null;

      // 自动执行注册流程
      if (node.authType === 'product') {
        node.log('一型一密注册流程启动');
        node.autoRegisterDevice();
      } else {
        node.log('一机一密直连流程启动');
        node.connectWithCredentials();
      }

      node.on('close', (removed, done) => {
        node.isClosing = true;
        node.connectionStatus = 'disconnected';

        if (node.reconnectTimer) {
          clearTimeout(node.reconnectTimer);
          node.reconnectTimer = null;
        }

        if (node.connectionTimeout) {
          clearTimeout(node.connectionTimeout);
          node.connectionTimeout = null;
        }

        if (node.mqttClient && node.boundMqttHandlers) {
          node.mqttClient.removeListener('connect', node.boundMqttHandlers.onConnectHandler);
          node.mqttClient.removeListener('offline', node.boundMqttHandlers.onOfflineHandler);
          node.mqttClient.removeListener('close', node.boundMqttHandlers.onCloseHandler);
          node.mqttClient.removeListener('error', node.boundMqttHandlers.onErrorHandler);
          node.mqttClient.removeListener('end', node.boundMqttHandlers.onEndHandler);
        }

        // 手动关闭 MQTT 连接
        if (node.mqttClient) {
          node.log('正在手动关闭 MQTT 连接...');
          node.mqttClient.end(false, () => {
            node.log('手动关闭MQTT连接');
            done();
          });
          return;
        }

        done();
      });
    }

    // 自动注册设备
    async autoRegisterDevice() {
      const node = this;
      try {
        node.status({ fill: 'yellow', shape: 'ring', text: '开始注册...' });

        // 生成临时密码
        const tempPassword = node.generateTempPassword();
        node.log('生成的临时密码:' + tempPassword);

        // 创建临时连接
        const tempClient = node.createTempConnection(tempPassword);
        node.log('临时连接已建立');

        // 执行注册
        node.status({ fill: 'yellow', shape: 'ring', text: '注册中...' });
        const credentials = await node.registerDevice(tempClient);
        node.log('注册成功，获得凭证:' + JSON.stringify(credentials));

        // 使用新凭证建立正式连接
        node.updateCredentials(credentials);
        node.connectWithCredentials();

      } catch (err) {
        node.status({
          fill: 'red',
          shape: 'ring',
          text: `注册失败: ${err.message.slice(0, 20)}...`
        });
        node.error('注册失败详情: ' + err.message);
      }
    }

    // 生成临时密码
    generateTempPassword() {
      const str = `${this.productSecret}productKey${this.productKey}timestamp2524608000000${this.productSecret}`;
      return crypto.createHmac('sha256', this.productSecret)
        .update(str)
        .digest('hex')
        .toUpperCase();
    }

    // 创建临时连接
    createTempConnection(password) {
      const options = {
        clientId: this.serialNumber,
        username: this.productKey,
        password: password,
        clean: true,
        reconnectPeriod: 0
      };
      return mqtt.connect(`mqtt://${this.server}:1883`, options);
    }

    // 执行设备注册
    registerDevice(client) {
      return new Promise((resolve, reject) => {
        // 添加响应超时
        const node = this;
        const responseTimeout = setTimeout(() => {
          reject(new Error('注册响应超时(15秒)'));
          client.end();
        }, 15000);
        const registerTopic = `/sys/${this.productKey}/${encodeURIComponent(this.serialNumber)}/register`;
        const replyTopic = registerTopic + '_reply';

        // 订阅响应Topic
        client.subscribe(replyTopic, (err) => {
          if (err) return reject(err);

          // 发送注册消息
          const message = {
            id: RED.util.generateId(),
            version: "1.0",
            method: `sys.${this.productKey}.${encodeURIComponent(this.serialNumber)}.register`,
            params: {
              name: this.serialNumber,
              serialNumber: this.serialNumber
            }
          };
          client.publish(registerTopic, JSON.stringify(message), { qos: 1 }, (err) => {
            if (err) {
              node.error("注册失败: " + err.message);
            } else {
              node.log('注册成功');
            }
          });
        });

        // 处理响应
        client.on('message', (topic, payload) => {
          if (topic === replyTopic) {
            clearTimeout(responseTimeout); // 清除超时
            try {
              const data = JSON.parse(payload.toString());
              node.log('收到注册响应:' + JSON.stringify(data));
              if (data.code === 20000 || data.code === 47001) {
                resolve(data.data);
              } else {
                reject(new Error(`${data.code}: ${data.message}`));
              }
            } catch (e) {
              reject(e);
            } finally {
              client.end();
            }
          }
        });
      });
    }

    // 更新凭证并保存
    updateCredentials(credentials) {
      const node = this;
      node.productKey = credentials.productKey;
      node.deviceName = credentials.deviceName;
      node.deviceSecret = credentials.deviceSecret;
      node.registered = true;

      // 保存到Node-RED上下文
      RED.nodes.addCredentials(node.id, {
        productKey: node.productKey,
        deviceName: node.deviceName,
        deviceSecret: node.deviceSecret
      });

      node.log('凭证已更新:' + JSON.stringify(credentials));
    }

    generateCredentials() {
      const node = this;
      const clientId = `${node.deviceName}&${node.productKey}`;
      const str = `clientId${node.productKey}.${node.deviceName}deviceName${node.deviceName}productKey${node.productKey}timestamp2524608000000`;
      const password = crypto
        .createHmac('sha256', node.deviceSecret)
        .update(str)
        .digest('hex')
        .toUpperCase();

      return { clientId, username: clientId, password };
    }

    // 使用凭证建立连接
    connectWithCredentials() {
      const node = this;

      // 关键修复：正确调用生成凭证方法
      const { clientId, username, password } = node.generateCredentials();
      node.log(`连接凭证: {clientId: ${clientId}, username: ${username}, password: ${password} }`);

      let url;
      switch (node.port) {  // 关键修复：使用node.port
        case '8883': url = `mqtts://${node.server}:${node.port}`; break;
        case '1883': url = `mqtt://${node.server}:${node.port}`; break;
        case '8083': url = `ws://${node.server}:${node.port}/mqtt`; break;
        case '8084': url = `wss://${node.server}:${node.port}/mqtt`; break;
        default: throw new Error('Invalid port');
      }

      const options = {
        clientId,
        username,
        password,
        keepalive: 25,
        clean: false,
        cleanSession: false,
        reconnectPeriod: 0,
        resubscribe: false // 保证客户端连接建立后重新发送subscribe包创建订阅，避免brocker端重启后订阅丢失，客户端由于缓存而无法重新建立订阅
      };

      node.mqttUrl = url;
      node.mqttOptions = options;

      const clearConnectionTimeout = () => {
        if (node.connectionTimeout) {
          clearTimeout(node.connectionTimeout);
          node.connectionTimeout = null;
        }
      };

      const startConnectionTimeout = () => {
        clearConnectionTimeout();
        node.connectionTimeout = setTimeout(() => {
          if (node.isClosing || !node.mqttClient) {
            return;
          }
          if (node.connectionStatus === 'connecting' || node.connectionStatus === 'reconnecting') {
            node.status({ fill: 'red', shape: 'ring', text: '连接超时' });
            node.error('MQTT连接超时');
            node.mqttClient.end(true);
          }
        }, 10000);
      };

      const scheduleReconnect = () => {
        if (node.isClosing || !node.mqttClient || node.reconnectTimer) {
          return;
        }

        node.reconnectAttempt += 1;
        const rawDelay = node.reconnectBaseDelay * Math.pow(2, node.reconnectAttempt - 1);
        const cappedDelay = Math.min(rawDelay, node.reconnectMaxDelay);
        const jitterFactor = 0.75 + (Math.random() * 0.5);
        const delay = Math.round(cappedDelay * jitterFactor);

        node.connectionStatus = 'reconnecting';
        node.status({ fill: 'yellow', shape: 'ring', text: `重连中(${Math.round(delay / 1000)}s)` });
        node.log(`MQTT将在 ${delay}ms 后进行第 ${node.reconnectAttempt} 次重连（基准延迟 ${cappedDelay}ms，抖动系数 ${jitterFactor.toFixed(2)}）`);

        node.reconnectTimer = setTimeout(() => {
          node.reconnectTimer = null;
          if (node.isClosing || !node.mqttClient) {
            return;
          }

          node.connectionStatus = 'reconnecting';
          startConnectionTimeout();
          node.log(`开始第 ${node.reconnectAttempt} 次MQTT重连`);
          node.mqttClient.emit('reconnect');
          node.mqttClient.reconnect();
        }, delay);
      };

      // 状态管理
      node.connectionStatus = 'connecting';
      node.status({ fill: 'yellow', shape: 'ring', text: '连接中...' });

      // 初始化连接：只创建一次 client，后续始终在同一个实例上手动重连，确保外部节点绑定的监听器不丢失
      if (!node.mqttClient) {
        node.mqttClient = mqtt.connect(url, options);
      }
      startConnectionTimeout();

      // 在构造函数内部，定义具名函数（确保在 close 回调中可以访问）
      const onConnectHandler = () => {
        clearConnectionTimeout();
        node.reconnectAttempt = 0;
        if (node.reconnectTimer) {
          clearTimeout(node.reconnectTimer);
          node.reconnectTimer = null;
        }
        node.connectionStatus = 'connected';
        node.status({ fill: 'green', shape: 'dot', text: '已连接' });
        node.log('MQTT连接成功');
      };

      const onOfflineHandler = () => {
        if (node.isClosing) {
          return;
        }
        node.log('MQTT已离线');
        node.connectionStatus = 'reconnecting';
        node.status({ fill: 'yellow', shape: 'ring', text: '连接已断开，等待重连' });
      };

      const onErrorHandler = (err) => {
        clearConnectionTimeout();
        node.connectionStatus = 'error';
        node.status({ fill: 'red', shape: 'ring', text: '连接错误' });
        node.error('MQTT错误: ' + err.message);
      };

      const onCloseHandler = () => {
        clearConnectionTimeout();
        if (node.isClosing) {
          node.connectionStatus = 'disconnected';
          node.log('MQTT已断开');
          node.status({ fill: 'grey', shape: 'ring', text: '已断开' });
          return;
        }

        node.log('MQTT连接关闭，准备手动重连');
        scheduleReconnect();
      };

      const onEndHandler = () => {
        clearConnectionTimeout();
        node.connectionStatus = 'disconnected';
        node.log('MQTT已断开');
        node.status({ fill: 'grey', shape: 'ring', text: '已断开' });
      };

      node.boundMqttHandlers = {
        onConnectHandler,
        onOfflineHandler,
        onCloseHandler,
        onErrorHandler,
        onEndHandler
      };

      // 绑定事件时使用具名函数
      node.mqttClient.on('connect', onConnectHandler);
      node.mqttClient.on('offline', onOfflineHandler);
      node.mqttClient.on('close', onCloseHandler);
      node.mqttClient.on('error', onErrorHandler);
      node.mqttClient.on('end', onEndHandler);
    }
  }

  // 注册节点并扩展验证规则
  RED.nodes.registerType('wulink-config', WulinkConfigNode);
};