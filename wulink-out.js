module.exports = function (RED) {
  function WulinkReportNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.configNode = RED.nodes.getNode(config.config);
    if (node.configNode?.mqttClient?.connected) {
      node.error('MQTT未连接');
      node.status({ fill: 'red', shape: 'ring', text: '未连接' });
    } else {
      const mqttClient = node.configNode?.mqttClient;
      const { productKey, deviceName } = node.configNode;
      // 状态管理
      mqttClient.on('connect', () => {
        node.status({ fill: 'green', shape: 'dot', text: '已连接' });
        node.log("成功连接到WuLink平台");
      });

      // 处理输入数据
      node.on('input', function (msg) {
        try {
          // 统一消息ID生成规则
          const messageId = msg.id || Date.now().toString();
          const { type, identifier, payload } = msg;
          let typeVal = type == undefined ? 'property' : type;
          // 根据类型分发处理逻辑
          switch (typeVal) {
            case 'property':
              handlePropertyReport(messageId, payload);
              break;
            case 'batchProperty':
              handlePropertyBatchReport(messageId, payload);
              break;
            case 'event':
              if (!identifier) throw new Error("事件上报需要提供identifier");
              handleEventReport(messageId, identifier, payload);
              break;
            case 'service':
              if (!identifier || !msg.originId) throw new Error("服务响应需要提供identifier和originId");
              handleServiceReply(msg.originId, identifier, payload, msg.code, msg.message);
              break;
            default:
              throw new Error(`未知的消息类型: ${typeVal}`);
          }
        } catch (e) {
          node.error("数据处理错误: " + e.message);
        }
      });

      // 属性批量上报
      const handlePropertyBatchReport = (id, data) => {
        const topic = `/sys/${productKey}/${deviceName}/thing/property/batch/post`;
        const params = data.map(a => {
          return formatPayload(a.payload, a.time);
        }).filter(a => Object.keys(a.values).length > 0);
        if (node.connectionStatus == 'connecting') {
          node.log("MQTT重连中");
        }
        for (let i = 0; i < params.length; i += 100) {
          const copyParams = params.slice(i, i + 100);
          const message = {
            id: Date.now().toString(),
            version: "1.0",
            method: "thing.property.batch.post",
            sentAt: Date.now(),
            params: copyParams
          };
          mqttClient.publish(topic, JSON.stringify(message), { qos: 1 }, (err) => {
            publishCallBack(err, topic, message);
          });
        }
      };

      // 属性上报处理
      const handlePropertyReport = (id, data) => {
        const topic = `/sys/${productKey}/${deviceName}/thing/property/post`;
        const message = {
          id,
          version: "1.0",
          method: "thing.property.post",
          params: formatPayload(data)
        };
        mqttClient.publish(topic, JSON.stringify(message), { qos: 1 }, (err) => {
          publishCallBack(err, topic, message);
        });
      };

      // 事件上报处理
      const handleEventReport = (id, identifier, data) => {
        const topic = `/sys/${productKey}/${deviceName}/thing/event/${identifier}/post`;
        const message = {
          id,
          version: "1.0",
          method: `thing.event.${identifier}.post`,
          params: formatPayload(data)
        };
        mqttClient.publish(topic, JSON.stringify(message), { qos: 1 }, (err) => {
          publishCallBack(err, topic, message);
        });
      };

      // 服务响应处理
      const handleServiceReply = (originId, identifier, data, code = 20000, responseMsg) => {
        const topic = `/sys/${productKey}/${deviceName}/thing/service/${identifier}/reply`;
        const message = {
          id: originId,
          version: "1.0",
          code,
          message: responseMsg,
          data
        };
        mqttClient.publish(topic, JSON.stringify(message), { qos: 1 }, (err) => {
          publishCallBack(err, topic, message);
        });
      };

      const publishCallBack = (err, topic, message) => {
        if (err) {
          node.error("上报失败: " + err.message);
          node.status({ fill: "red", shape: "ring", text: "上报失败" });
        } else {
          node.log(`成功向${topic}发布消息：${JSON.stringify(message)}`);
          node.status({ fill: "blue", shape: "dot", text: "已上报" });
          setTimeout(() => {
            node.status({}); // 2秒后清除状态
          }, 2000);
        }
      };

      // 数据格式化工具
      const formatPayload = (data, time) => {
        return {
          time: time == undefined ? Date.now() : time,
          values: data
        };
      };

      // 监听配置节点的连接状态变化
      if (node.configNode) {
        node.configNode.on('status', (status) => {
          node.status(status); // 同步显示连接状态
        });
      }

      // 节点关闭处理
      node.on('close', () => {
        node.status({});
      });
    }
  }
  RED.nodes.registerType("wulink-out", WulinkReportNode);
};