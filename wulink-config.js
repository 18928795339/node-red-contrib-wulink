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
      node.retryCount = 0;
      node.reconnectTimer = null;
      node.INITIAL_RETRY_DELAY = 2000;   // 初始等待2秒
      node.MAX_RETRY_DELAY = 60000;      // 最大等待60秒
      node.MAX_RETRY_ATTEMPTS = 10;      // 最多重试10次（可选）

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

      // 在构造函数末尾添加
      node.on('close', (removed, done) => {
        // 清理重连定时器
        if (node.reconnectTimer) {
          clearTimeout(node.reconnectTimer);
          node.reconnectTimer = null;
        }
        // 手动关闭 MQTT 连接
        if (node.mqttClient && node.mqttClient.connected) {
          node.log('正在手动关闭 MQTT 连接...');
          // 使用 end 优雅关闭，并等待完成
          node.connectionStatus = 'disconnected';
          node.mqttClient.end(false, () => {
            node.log('手动关闭MQTT连接');
            done();
          });
        } else {
          done();
        }
      });
    }

    calculateDelay() {
      const node = this;
      let delay = Math.min(
        node.INITIAL_RETRY_DELAY * Math.pow(2, node.retryCount),
        node.MAX_RETRY_DELAY
      );
      // 添加±10%随机抖动，避免大量设备同时重连
      const jitter = delay * (0.9 + Math.random() * 0.2);
      return Math.floor(jitter);
    }

    scheduleReconnect() {
      const node = this;
      if (node.reconnectTimer) clearTimeout(node.reconnectTimer);
      if (node.MAX_RETRY_ATTEMPTS && node.retryCount >= node.MAX_RETRY_ATTEMPTS) {
        node.error(`已达到最大重试次数 (${node.MAX_RETRY_ATTEMPTS})，停止重连。`);
        node.status({ fill: 'red', shape: 'ring', text: '重连失败，已达上限' });
        return;
      }
      const delay = node.calculateDelay();
      node.log(`MQTT 连接断开，第 ${node.retryCount + 1} 次重连将在 ${(delay / 1000).toFixed(1)} 秒后执行。`);
      node.status({ fill: 'yellow', shape: 'ring', text: `重连等待 ${(delay / 1000).toFixed(0)}s` });
      node.reconnectTimer = setTimeout(() => {
        node.log(`正在执行第 ${node.retryCount + 1} 次重连...`);
        if (node.mqttClient && typeof node.mqttClient.reconnect === 'function') {
          node.mqttClient.reconnect();
        }
        node.retryCount++;
      }, delay);
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
        reconnectPeriod: 0
      };

      // 状态管理
      node.connectionStatus = 'connecting';
      // 添加连接超时检测
      const connectionTimeout = setTimeout(() => {
        if (node.connectionStatus === 'connecting') {
          node.status({ fill: 'red', text: '连接超时' });
          node.error('MQTT连接超时');
          if (node.mqttClient) node.mqttClient.end(true);
        }
      }, 10000); // 10秒超时
      node.status({ fill: 'yellow', shape: 'ring', text: '连接中...' });

      // 初始化连接
      node.mqttClient = mqtt.connect(url, options);

      // 事件监听
      node.mqttClient.on('connect', () => {
        clearTimeout(connectionTimeout);
        node.connectionStatus = 'connected';
        node.retryCount = 0;
        if (node.reconnectTimer) {
          clearTimeout(node.reconnectTimer);
          node.reconnectTimer = null;
        }
        node.status({ fill: 'green', shape: 'dot', text: '已连接' });
        node.log('MQTT连接成功');
      });

      node.mqttClient.on('offline', () => {
        node.log('MQTT客户端进入离线状态，准备指数退避重连');
        node.scheduleReconnect();
      });

      node.mqttClient.on('reconnect', () => {
        node.log('MQTT正在尝试重连...');
        node.status({ fill: 'yellow', shape: 'ring', text: '重连中...' });
      });

      node.mqttClient.on('error', (err) => {
        node.connectionStatus = 'error';
        node.status({ fill: 'red', shape: 'ring', text: '连接错误' });
        node.error('MQTT错误: ' + err.message);
        // 若尚未处于重连调度中且连接未关闭，则启动重连
        if (!node.reconnectTimer && node.mqttClient && !node.mqttClient.connected) {
          node.scheduleReconnect();
        }
      });

      node.mqttClient.on('close', () => {
        if (node.connectionStatus === 'error') {
          return;
        }

        if (node.connectionStatus !== 'disconnected') {
          node.connectionStatus = 'reconnecting';
          node.connectionStatus = 'reconnecting';
          node.log('MQTT连接关闭，调度重连');
          node.scheduleReconnect();
        }
      });

      node.mqttClient.on('end', () => {
        node.connectionStatus = 'disconnected';
        node.log('MQTT已断开');
        node.status({ fill: 'grey', shape: 'ring', text: '已断开' });
      });
    }
  }

  // 注册节点并扩展验证规则
  RED.nodes.registerType('wulink-config', WulinkConfigNode);
};