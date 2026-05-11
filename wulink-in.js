module.exports = function (RED) {
  function WulinkWriteNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.configNode = RED.nodes.getNode(config.config);
    const mqttClient = node.configNode?.mqttClient;

    if (mqttClient?.connected) {
      node.error('MQTT未连接');
      node.status({ fill: 'red', shape: 'ring', text: '未连接' });
    } else {
      const { productKey, deviceName } = node.configNode;
      // 设置属性Topic
      const setTopic = `/sys/${productKey}/${deviceName}/thing/property/set`;
      // 服务调用topic
      const serviceTopic = `/sys/${productKey}/${deviceName}/thing/service/+`;

      mqttClient.on('connect', () => {
        node.status({ fill: 'green', shape: 'dot', text: '监听中' });
        // 订阅属性设置
        mqttClient.subscribe(setTopic, { qos: 0 }, (err) => {
          if (!err) node.log(`已订阅设置属性Topic: ${setTopic}`);
        });
        // 订阅服务调用
        mqttClient.subscribe(serviceTopic, { qos: 0 }, (err) => {
          if (!err) node.log(`已订阅服务调用topic: ${serviceTopic}`);
        });
      });

      // 处理平台下发指令
      mqttClient.on('message', (topic, message) => {
        console.log('收到指令:', message.toString());
        const payload = JSON.parse(message.toString());
        try {
          if (topic === setTopic) {
            // 构造标准化输出
            const outputMsg = {
              topic: "property/set",
              payload: payload.params.values,
              original: payload
            };
            // 发送响应
            const replyTopic = `/sys/${productKey}/${deviceName}/thing/property/set_reply`;
            const response = {
              id: payload.id,
              version: "1.0",
              method: "thing.property.set_reply",
              code: 20000,
              message: "success",
              data: {}
            };
            mqttClient.publish(replyTopic, JSON.stringify(response), { qos: 1 });

            // 输出到流程
            node.send(outputMsg);
            node.status({ fill: "blue", shape: "dot", text: "收到新指令" });
          } else if (topic.includes('/thing/service/')) {
            const identifier = topic.split('/')[6]; // 提取服务标识符
            // 发送到输出端口
            node.send({
              topic: topic,
              type: 'service',
              identifier,
              payload: payload.params.values,
              _origin: payload, // 保留原始消息用于响应
              _msgid: RED.util.generateId()
            });
          }
        } catch (e) {
          node.error("指令解析错误: " + e.message);
        }
      });

      // 监听配置节点的连接状态变化
      if (node.configNode) {
        node.configNode.on('status', (status) => {
          node.status(status); // 同步显示连接状态
        });
      }

      // 节点关闭处理
      node.on('close', () => {
        if (node.configNode?.mqttClient) {
          node.configNode.mqttClient.removeListener('message', handleMessage);
        }
        node.status({});
      });
    }
  }
  RED.nodes.registerType("wulink-in", WulinkWriteNode);
};