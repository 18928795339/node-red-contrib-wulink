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

      // 自动执行注册流程
      if (node.authType === 'product') {
        node.log('一型一密注册流程启动');
        node.autoRegisterDevice();
      } else {
        node.log('一机一密直连流程启动');
        node.connectWithCredentials();
      }
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
        reconnectPeriod: 5000
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

          client.publish(registerTopic, JSON.stringify(message));
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
        keepalive: 60,
        clean: true,
        reconnectPeriod: 60000
      };

      // 状态管理
      node.connectionStatus = 'connecting';
      // 添加连接超时检测
      const connectionTimeout = setTimeout(() => {
        if (node.connectionStatus === 'connecting') {
          node.status({ fill: 'red', text: '连接超时' });
          node.error('MQTT连接超时');
          node.mqttClient.end();
        }
      }, 10000); // 10秒超时
      node.status({ fill: 'yellow', shape: 'ring', text: '连接中...' });

      // 初始化连接
      node.mqttClient = mqtt.connect(url, options);

      // 事件监听
      node.mqttClient.on('connect', () => {
        clearTimeout(connectionTimeout);
        node.connectionStatus = 'connected';
        node.status({ fill: 'green', shape: 'dot', text: '已连接' });
        node.log('MQTT连接成功');
      });

      node.mqttClient.on('error', (err) => {
        node.connectionStatus = 'error';
        node.status({ fill: 'red', shape: 'ring', text: '连接错误' });
        node.error('MQTT错误: ' + err.message);
      });

      node.mqttClient.on('close', () => {
        if (node.connectionStatus === 'error') {
          return;
        }

        if (node.connectionStatus !== 'disconnected') {
          node.connectionStatus = 'reconnecting';
          node.status({ fill: 'yellow', shape: 'ring', text: '重连中...' });
        }
      });

      node.mqttClient.on('end', () => {
        node.connectionStatus = 'disconnected';
        node.status({ fill: 'grey', shape: 'ring', text: '已断开' });
      });
    }
  }

  // 注册节点并扩展验证规则
  RED.nodes.registerType('wulink-config', WulinkConfigNode);
};