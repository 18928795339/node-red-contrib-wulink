const crypto = require('crypto');
const axios = require('axios');
const mqtt = require('mqtt');

module.exports = function (RED) {
  function FlowUpdateNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.configNode = RED.nodes.getNode(config.config);
    const mqttClient = node.configNode?.mqttClient;
    if (mqttClient?.connected) {
      node.error('MQTT未连接');
      node.status({ fill: 'red', shape: 'ring', text: '未连接' });
    } else {
      // 配置参数
      node.log(node.configNode);
      const { productKey, deviceName } = node.configNode;
      const mqTopic = `/sys/${productKey}/${deviceName}/thing/config/push`;
      const otaTopic = '/ota/update/push'

      // 订阅配置Topic
      mqttClient.on('connect', () => {
        node.status({ fill: 'green', shape: 'dot', text: '已连接' });
        mqttClient.subscribe(mqTopic, { qos: 1 }, (err) => {
          if (!err) node.log(`已订阅配置下发Topic: ${mqTopic}`);
        });
        mqttClient.subscribe(otaTopic, { qos: 1 }, (err) => {
          if (!err) node.log(`已订阅OTA更新Topic: ${otaTopic}`);
        });
      });

      class FlowsCreateUtil {
        getFlows(configData) {
          let hasRead = false;
          let hasWrite = false;
          const writeNodeIdMap = {};
          const flows = [...this.getNetworkPortNodes(configData.networkPortSlaveConfigs), ...this.getSlaveConfigNodes(configData.slaveConfigs)]
          // 判断是否有读写节点并提取 serialPortNumber -> id 映射
          for (const nodeConfig of flows) {
            const type = nodeConfig.type;
            if (type === "modbus-flex-getter" || type === "s7 in") {
              hasRead = true;
            } else if (type === "modbus-flex-write" || type === "s7 out") {
              hasWrite = true;
            }
            if (type === "function") {
              const name = nodeConfig.name;
              if (name === "requestFormat" || name === "networkRequestFormat") {
                const serialPortNumber = nodeConfig.serialPortNumber;
                writeNodeIdMap[serialPortNumber] = nodeConfig.id;;
              }
            }
          }
          // 添加上报相关节点
          if (hasRead) {
            const reportAndClearDataNode = this.getReportAndClearDataNode(configData.id);
            reportAndClearDataNode.needReport = true;
            const reportInjectNode = this.getInjectNode(
              reportAndClearDataNode.id,
              configData.reportCycle,
            );
            const reportAtBreakPointNode = this.getInjectNode(
              reportAndClearDataNode.id,
              undefined,
              this.getProps("payload", true, "bool"),
              "reportAtBreakPoint"
            );
            flows.push(reportAndClearDataNode);
            flows.push(reportInjectNode);
            flows.push(reportAtBreakPointNode);
          }
          // 添加写入相关节点
          if (hasWrite) {
            const serialNumbers = [];
            const writeIds = [];
            for (const [key, value] of Object.entries(writeNodeIdMap)) {
              serialNumbers.push(key);
              writeIds.push(value);
            }

            const switchNode = this.getSwitchNode(writeIds, String(configData.id), serialNumbers);
            const splitNode = this.getSplitNode(switchNode.id, String(configData.id));
            const createWriteNode = this.getCreateWriteRequestNode(splitNode.id, configData.id);
            createWriteNode.needWrite = true;

            flows.push(switchNode);
            flows.push(splitNode);
            flows.push(createWriteNode);
          }

          // 设置坐标
          let x = 500;
          let y = 150;
          for (const nodeConfig of flows) {
            const type = nodeConfig.type;
            if (type !== "s7 endpoint" && type !== "modbus-client") {
              nodeConfig.x = x;
              nodeConfig.y = y;
              y += 50;
            }
          }

          return flows;
        }

        getNetworkPortNodes(configs) {
          const nodes = [];
          const PROTOCOL_TYPES = ['smart-200', 'S71200_1500', 'MODBUS_TCP'];
          for (const config of configs) {
            const endpoint = config.endpoint;
            if (PROTOCOL_TYPES[config.protocol] != 'MODBUS_TCP') {
              // 创建节点对象
              const objectNode = {
                id: endpoint,
                name: `${config.name}-配置`,
                type: "s7 endpoint",
                transport: "iso-on-tcp",
                address: config.ipAddress,
                port: config.port,
                localtsaphi: config.localtsaphi ?? "01",
                localtsaplo: config.localtsaplo ?? "00",
                remotetsaphi: config.remotetsaphi ?? "01",
                remotetsaplo: config.remotetsaplo ?? "00",
                slot: config.slot ?? 1,
                rack: config.rack ?? 0,
                cycletime: config.cycleTime,
                timeout: config.timeout,
                connmode: PROTOCOL_TYPES[config.protocol] == 'S71200_1500' ? "rack-slot" : "tsap",
                vartable: config.vartable || [],
                adapter: "",
                busaddr: 2
              };
              nodes.push(objectNode);

              // 添加 in/out 节点
              if (config.hasRead) {
                const cacheDataNode = this.getFilterAndCacheDataNode(endpoint);
                const inNode = this.getInNode(endpoint, cacheDataNode.id);
                nodes.push(inNode, cacheDataNode);
              }

              if (config.hasWrite) {
                const outNode = this.getOutNode(endpoint);
                const requestFormatNode = this.getNetworkRequestFormatNode(outNode.id, endpoint);
                requestFormatNode.serialPortNumber = config.serialPortNumber;
                nodes.push(outNode, requestFormatNode);
              }
            } else {
              nodes.push(...this.getModbusTcpNodes(config));
            }
          }
          return nodes;
        }

        getSlaveConfigNodes(configs) {
          const nodes = [];
          const clientNodeMap = {}; // serialPortNumber -> requestCreateNodeId
          for (const config of configs) {
            const endpoint = config.endpoint;
            let hasWrite = config.hasWrite;
            let hasRead = config.hasRead;

            const serialPortNumber = config.serialPortNumber;

            if (clientNodeMap[serialPortNumber] != undefined) {
              const requestCreateNodeId = clientNodeMap[serialPortNumber];
              if (hasRead) {
                const injectNode = this.getInjectNode(
                  requestCreateNodeId,
                  config.cycleTime,
                  this.getProps("configKey", endpoint, "text")
                );
                nodes.push(injectNode);
              }
            } else {
              // 创建 Modbus 节点
              const objectNode = this.getModbusNode();
              objectNode.id = endpoint;
              objectNode.name = `${config.name}-配置`;
              objectNode.clienttype = "simpleser";
              objectNode.serialPort = config.serialPort;
              objectNode.tcpType = "DEFAULT";

              // 设置串口参数
              objectNode.serialBaudrate = config.serialBaudrate;
              objectNode.serialDatabits = config.serialDatabits;
              objectNode.serialStopbits = config.serialStopbits;
              objectNode.serialParity = config.serialParity;
              objectNode.clientTimeout = config.timeout;

              nodes.push(objectNode);

              if (hasRead) {
                const dataParserAndFilterNode = this.getDataParserAndFilterNode(undefined, endpoint);
                const mergeNode = this.getDataMergeNode(dataParserAndFilterNode.id, endpoint);
                const getterNode = this.getGetterNode(mergeNode.id, endpoint, config.name);
                const requestCreateNode = this.getRequestCreateNode(getterNode.id, endpoint);
                // 记录 requestCreateNode ID 到 map
                clientNodeMap[serialPortNumber] = requestCreateNode.id;

                const collectInjectNode = this.getInjectNode(
                  requestCreateNode.id,
                  config.cycleTime,
                  this.getProps("configKey", endpoint, "text")
                );
                nodes.push(dataParserAndFilterNode, mergeNode, getterNode, requestCreateNode, collectInjectNode);
              }

              if (hasWrite) {
                const writeNode = this.getWriteNode(endpoint);
                const requestFormatNode = this.getRequestFormatNode(writeNode.id, endpoint);
                requestFormatNode.serialPortNumber = serialPortNumber;

                nodes.push(writeNode, requestFormatNode);
              }
            }
          }
          return nodes;
        }
        getModbusTcpNodes(config) {
          // 创建 Modbus 节点
          const nodes = [];
          const endpoint = config.endpoint;
          const objectNode = this.getModbusNode();
          objectNode.id = endpoint;
          objectNode.name = `${config.name}-配置`;
          objectNode.tcpHost = config.ipAddress;
          objectNode.tcpPort = config.port;
          objectNode.clientTimeout = config.timeout;
          objectNode.clienttype = "tcp";
          objectNode.tcpType = "DEFAULT";
          nodes.push(objectNode);

          let hasWrite = config.hasWrite;
          let hasRead = config.hasRead;
          // 如果有可读属性，添加相关节点
          if (hasRead) {
            const dataParserAndFilterNode = this.getDataParserAndFilterNode(undefined, endpoint);
            const mergeNode = this.getDataMergeNode(dataParserAndFilterNode.id, endpoint);
            const getterNode = this.getGetterNode(mergeNode.id, endpoint, config.name);
            const requestCreateNode = this.getRequestCreateNode(getterNode.id, endpoint);
            const injectNode = this.getInjectNode(
              requestCreateNode.id,
              config.cycleTime,
              this.getProps("configKey", endpoint, "text")
            );
            const collectInjectNode = this.getInjectNode(
              requestCreateNode.id,
              config.cycleTime,
              this.getProps("configKey", endpoint, "text")
            );

            nodes.push(dataParserAndFilterNode);
            nodes.push(mergeNode);
            nodes.push(getterNode);
            nodes.push(requestCreateNode);
            nodes.push(collectInjectNode);
            nodes.push(injectNode);
          }

          // 如果有可写属性，添加写入节点
          if (hasWrite) {
            const writeNode = this.getWriteNode(endpoint);
            const requestFormatNode = this.getRequestFormatNode(writeNode.id, endpoint);
            requestFormatNode.serialPortNumber = config.serialPortNumber;

            nodes.push(writeNode);
            nodes.push(requestFormatNode);
          }
          return nodes;
        }

        getInNode(endpoint, cacheId) {
          const nodeConfig = {
            id: `${endpoint}-in-normal`,
            type: "s7 in",
            endpoint,
            mode: "all",
            variable: "",
            diff: false,
            name: `${endpoint}-in-normal`,
          };
          this.setWriesArray(nodeConfig, [cacheId]);
          return nodeConfig;
        }

        getOutNode(endpoint) {
          return {
            id: `${endpoint}-out`,
            type: "s7 out",
            endpoint,
            name: `${endpoint}-out`,
            variable: "",
            needRead: true,
          };
        }

        getFilterAndCacheDataNode(configKey) {
          const funScript =
            "const parseValue = (value) => {\n  // 判断是否为数字，且为有限数值（排除 NaN、Infinity、-Infinity）\n  if (typeof value === 'number' && isFinite(value)) {\n      // 使用 toFixed(6) 得到四舍五入后的字符串，再转换为数字\n      const rounded = Number(value.toFixed(6));\n      // 如果舍入前后的值不相等，说明原数值小数位数超过6位，返回舍入后的值\n      if (value !== rounded) {\n          return rounded;\n      }\n  }\n  return value;\n};\n\nif (flow.get('configs') == undefined){\n  const configs = flow.get('configs', 'file');\n    if (configs == undefined){\n      node.log('获取配置为空');\n      return null;   \n    } else {\n    flow.set('configs', configs);\n    }\n    node.log('重新加载上报设置配置');\n}\nconst reportTypes = ['正常上报', '变化即上报', '差值过量上报'];\nlet data = msg.payload;\nconst configKey = node.id.substring(0, node.id.length - \"filterAndCacheData\".length - 1);\nconst reportSetting = flow.get('configs')[configKey];\nconst lastDataCacheKey = configKey + '-lastData';\nlet lastData = flow.get(lastDataCacheKey);\nif (lastData == undefined){\n  lastData = flow.get(lastDataCacheKey, 'file');\n}\nif (lastData != undefined) {\n  const filterData = {};\n    Object.keys(data).forEach(key => {\n      data[key] = parseValue(data[key]);\n      const currentValue = data[key];\n      const historyValue = lastData[key];\n      if (reportTypes[reportSetting[key].reportSetting] == '正常上报') {\n        filterData[key] = currentValue;\n      } else if (reportTypes[reportSetting[key].reportSetting] == '变化即上报'){\n        if (currentValue !== historyValue) {\n          filterData[key] = currentValue;\n        }\n      } else {\n        if (Math.abs(currentValue - historyValue) >= reportSetting[key].differenceThreshold) {\n          filterData[key] = currentValue;\n        }\n      }\n  });\n  data = filterData;\n}\n/** 更新缓存的最新数据 */\nflow.set(lastDataCacheKey, msg.payload);\nif (flow.get(\"reportAtBreakPoint\")){\n  flow.set(lastDataCacheKey, msg.payload, 'file');\n}\nconst reportDataCacheKey = node.id + '-reportData';\nconst reportData = flow.get(reportDataCacheKey) ?? [];\nreportData.push({\n    time: Date.now(),\n    data: data,\n});\nif(reportData.length > 100000){\n  reportData.shift();\n}\nnode.log('s7-smrt缓存数据:' + JSON.stringify(data));\nflow.set(reportDataCacheKey, reportData);\nif (flow.get(\"reportAtBreakPoint\")) {\n  flow.set(reportDataCacheKey, reportData, 'file');\n}\nreturn msg;";
          return this.getRunFunctionNode(undefined, configKey, funScript, "filterAndCacheData");
        }

        getReportAndClearDataNode(configKey) {
          const funcScript =
            "try {\n    node.log('上报已采集的数据');\n    const allReportData = [];\n    if (msg.payload) {\n        /** node-red恢复运行后立刻上报上次缓存中未上报的数据 */ // 获取名为 'file' 的存储对象\n        let keys = flow.keys('file');\n        for (const key of keys) {\n            if (key.endsWith('-reportData')) {\n                const reportData = flow.get(key, 'file') ?? [];\n                allReportData.push(...reportData);\n                flow.set(key, undefined, 'file');\n            } else if (key.endsWith('-lastData')) {\n                flow.set(key, undefined, 'file');\n            }\n\n        }\n    } else {\n        for (const key of flow.keys()) {\n            if (key.endsWith('-reportData')) {\n                const reportData = flow.get(key) ?? [];\n                allReportData.push(...reportData);\n                flow.set(key, []);\n            }\n        }\n    }\n    if (allReportData.length == 0) {\n        return;\n    }\n    const timeMap = new Map();\n    for (const item of allReportData) {\n        if (timeMap.has(item.time)) {\n            timeMap.set(item.time, { ...timeMap.get(item.time), ...item.data });\n        } else {\n            timeMap.set(item.time, item.data);\n        }\n    }\n    const sortedEntries = [...timeMap].sort((a, b) => a[0] - b[0]);\n    const sortedReportData = sortedEntries.map(entry => {\n        return {\n            time: entry[0],\n            payload: entry[1]\n        }\n    });\n    node.send({ ...msg, type: 'batchProperty', payload: sortedReportData });\n    return null;\n} catch (err) {\n    node.error('上报数据持久化失败: ' + err.message);\n    return null;\n}";
          return this.getRunFunctionNode(undefined, configKey, funcScript, "reportAndClearData");
        }

        getDataParserAndFilterNode(writeId, configKey) {
          const funcScript =
            "const DATA_TYPES = {\n  HEX: 0, INT16: 1, UINT16: 2, INT32: 3, UINT32: 4, INT64: 5,\n  FLOAT32: 6, FLOAT64: 7, BOOL: 8, UTF8: 9, BYTE: 10,\n  UINT64: 11, GBK: 12\n};\nconst TRANSFER_MODE_RTU = 'RTU';\nconst DATA_ENCODING = {\n  ABCD: 1, BADC: 2,\n  CDAB: 3, DCBA: 4\n};\n\n\nconst iconv = global.get('iconv');\n\nif (!flow.get('bufferParser')) {\n  flow.set('bufferParser', {\n    // 十进制转换（支持跨节点复用）\n    dataEncode: DATA_ENCODING.ABCD,\n    convertToRealValue: function (value) {\n      // 判断是否为数字，且为有限数值（排除 NaN、Infinity、-Infinity）\n      if (typeof value === 'number' && isFinite(value)) {\n          // 使用 toFixed(6) 得到四舍五入后的字符串，再转换为数字\n          const rounded = Number(value.toFixed(6));\n          // 如果舍入前后的值不相等，说明原数值小数位数超过6位，返回舍入后的值\n          if (value !== rounded) {\n              return rounded;\n          }\n      }\n      return value;\n    },\n    // 核心解析逻辑（包含字节序处理）\n    readValue: function (buffer, dataType) {\n      const parser = this;\n      switch (dataType) {\n        case DATA_TYPES.HEX:\n          return parser._readHex(buffer);\n        case DATA_TYPES.BOOL:\n          return parser._readBit(buffer);\n        case DATA_TYPES.BYTE:\n          return parser._readInt8(buffer);\n        case DATA_TYPES.UINT16:\n          return parser._readUint16(buffer);\n        case DATA_TYPES.INT16:\n          return parser._readInt16(buffer);\n        case DATA_TYPES.UINT32:\n          return parser._readUint32(buffer);\n        case DATA_TYPES.INT32:\n          return parser._readInt32(buffer);\n        case DATA_TYPES.FLOAT32:\n          return parser.convertToRealValue(parser._readFloat32(buffer));\n        case DATA_TYPES.INT64:\n          return parser._readInt64(buffer);\n        case DATA_TYPES.UINT64:\n          return parser._readUInt64(buffer);\n        case DATA_TYPES.FLOAT64:\n          return parser.convertToRealValue(parser._readFloat64(buffer));\n        case DATA_TYPES.UTF8:\n          return parser._read(buffer, 'utf-8');\n        case DATA_TYPES.GBK:\n          return parser._read(buffer, 'gbk');\n        default:\n          throw new Error('不支持的 dataType:' + dataType);\n      }\n    },\n\n    // 具体解析方法（私有方法前缀_）\n    _readHex: function (buffer) {\n      return Array.from(new Uint8Array(buffer),\n        byte => ('0' + byte.to(16).toUpperCase()).slice(-2)).join(' ');\n    },\n\n    _readBit: function (buffer) {\n      return new DataView(buffer).getUint8(0) & 0x01;\n    },\n\n    _readInt8: function (buffer) {\n      const dv = new DataView(buffer);\n      return dv.getInt8(0);\n    },\n\n    _readUint16: function (buffer) {\n      const dv = new DataView(buffer);\n      switch (this.dataEncode) {\n        case DATA_ENCODING.ABCD: return dv.getUint16(0);\n        case DATA_ENCODING.BADC: return dv.getUint16(0, true);\n        default:\n          throw new Error('不支持的字节序:' + this.dataEncode);\n      }\n    },\n    _readInt16: function (buffer) {\n      const dv = new DataView(buffer);\n      switch (this.dataEncode) {\n        case DATA_ENCODING.ABCD:\n          return dv.getInt16(0);\n        case DATA_ENCODING.BADC:\n          return dv.getInt16(0, true);\n        default:\n          throw new Error('不支持的数据字节序');\n      }\n    },\n\n    _readInt32: function (buffer) {\n      const dv = new DataView(buffer);\n      switch (this.dataEncode) {\n        case DATA_ENCODING.ABCD:\n          // 大端 32 位直接读取\n          return dv.getUint32(0, false);\n\n        case DATA_ENCODING.BADC: {\n          // 先按大端取出两个寄存器值\n          let reg0 = dv.getUint16(0, false); // 原高 16 位（已交换字节）\n          let reg1 = dv.getUint16(2, false); // 原低 16 位（已交换字节）\n          // 各自交换高低字节，还原原始的高/低 16 位\n          let origHigh = ((reg0 << 8) & 0xff00) | ((reg0 >> 8) & 0x00ff);\n          let origLow = ((reg1 << 8) & 0xff00) | ((reg1 >> 8) & 0x00ff);\n          return (origHigh << 16) | origLow;\n        }\n\n        case DATA_ENCODING.CDAB: {\n          // 寄存器顺序：低 16 位在前，高 16 位在后\n          let low = dv.getUint16(0, false);\n          let high = dv.getUint16(2, false);\n          return (high << 16) | low;\n        }\n\n        case DATA_ENCODING.DCBA:\n          // 小端 32 位直接读取\n          return dv.getUint32(0, true);\n\n        default:\n          throw new Error('不支持的数据字节序');\n      }\n    },\n\n    _readUint32: function (buffer) {\n      return this._readInt32(buffer) >>> 0;\n    },\n\n    _readFloat32: function (buffer) {\n      const int32 = new Int32Array([this._readInt32(buffer)]);\n      return new Float32Array(int32.buffer)[0];\n    },\n\n    _readInt64: function (buffer) {\n      const dv = new DataView(buffer);\n      switch (this.dataEncode) {\n        case DATA_ENCODING.ABCD:\n          return dv.getBigInt64(0);\n        case DATA_ENCODING.BADC: {\n          const buf = new Uint8Array([\n            dv.getUint8(1), dv.getUint8(0),\n            dv.getUint8(3), dv.getUint8(2),\n            dv.getUint8(5), dv.getUint8(4),\n            dv.getUint8(7), dv.getUint8(6),\n          ]);\n          return new DataView(buf.buffer).getBigInt64(0);\n        }\n        case DATA_ENCODING.CDAB: {   // 修正\n          const buf = new Uint8Array([\n            dv.getUint8(4), dv.getUint8(5),\n            dv.getUint8(6), dv.getUint8(7),\n            dv.getUint8(0), dv.getUint8(1),\n            dv.getUint8(2), dv.getUint8(3),\n          ]);\n          return new DataView(buf.buffer).getBigInt64(0);\n        }\n        case DATA_ENCODING.DCBA:\n          return dv.getBigInt64(0, true);\n        default:\n          throw new Error('不支持的数据字节序');\n      }\n    },\n\n    _readUint64: function (buffer) {\n      const dv = new DataView(buffer);\n      switch (this.dataEncode) {\n          case DATA_ENCODING.ABCD:\n              return dv.getBigUint64(0);\n          case DATA_ENCODING.BADC: {\n              const buf = new Uint8Array([\n                  dv.getUint8(1), dv.getUint8(0),\n                  dv.getUint8(3), dv.getUint8(2),\n                  dv.getUint8(5), dv.getUint8(4),\n                  dv.getUint8(7), dv.getUint8(6),\n              ]);\n              return new DataView(buf.buffer).getBigUint64(0);\n          }\n          case DATA_ENCODING.CDAB: {\n              const buf = new Uint8Array([\n                  dv.getUint8(4), dv.getUint8(5),\n                  dv.getUint8(6), dv.getUint8(7),\n                  dv.getUint8(0), dv.getUint8(1),\n                  dv.getUint8(2), dv.getUint8(3),\n              ]);\n              return new DataView(buf.buffer).getBigUint64(0);\n          }\n          case DATA_ENCODING.DCBA:\n              return dv.getBigUint64(0, true);\n          default:\n              throw new Error('不支持的数据字节序');\n      }\n    },\n\n    _readFloat64: function (buffer) {\n      const dv = new DataView(buffer);\n      switch (this.dataEncode) {\n          case DATA_ENCODING.ABCD:\n              return dv.getFloat64(0);\n          case DATA_ENCODING.BADC: {\n              const buf = new Uint8Array([\n                  dv.getUint8(1), dv.getUint8(0),\n                  dv.getUint8(3), dv.getUint8(2),\n                  dv.getUint8(5), dv.getUint8(4),\n                  dv.getUint8(7), dv.getUint8(6),\n              ]);\n              return new DataView(buf.buffer).getFloat64(0);\n          }\n          case DATA_ENCODING.CDAB: {   // 修正\n              const buf = new Uint8Array([\n                  dv.getUint8(4), dv.getUint8(5),\n                  dv.getUint8(6), dv.getUint8(7),\n                  dv.getUint8(0), dv.getUint8(1),\n                  dv.getUint8(2), dv.getUint8(3),\n              ]);\n              return new DataView(buf.buffer).getFloat64(0);\n          }\n          case DATA_ENCODING.DCBA:\n              return dv.getFloat64(0, true);\n          default:\n              throw new Error('不支持的数据字节序');\n      }\n    },\n\n    _read: function (arrayBuffer, encodeType) {\n      // 2. 将 ArrayBuffer 转为 Uint8Array 以便按字节操作\n      let bytes = new Uint8Array(arrayBuffer);\n      // 3. 根据字节序交换每个16位字的两个字节（如果需要）\n      // 假设 DATA_ENCODING.ABCD 代表大端模式，this.dataEncode 为当前设定的字节序\n      if (DATA_ENCODING.ABCD != this.dataEncode) {\n          // 小端模式：交换每个字的高低位\n          for (let i = 0; i < bytes.length; i += 2) {\n              let temp = bytes[i];\n              bytes[i] = bytes[i + 1];\n              bytes[i + 1] = temp;\n          }\n      }\n      // 4. 解码：将处理后的字节 Buffer 转为字符串\n      const buffer = Buffer.from(bytes);   // 转换为 Node.js Buffer\n      let str;\n      if (encodeType === 'utf8' || encodeType === 'utf-8') {\n          str = buffer.toString('utf8');\n      } else {\n          const iconv = global.get('iconv'); // 确保已在 settings.js 中注入\n          str = iconv.decode(buffer, encodeType);\n      }\n      // 5. 去除末尾的 \\0 字符\n      str = str.replace(/\\0+$/, '');\n      return str;\n    }\n  });\n}\n\n// 主处理逻辑\nconst parser = flow.get('bufferParser');\ntry {\n  const configKey = msg.configKey;\n  const configs = flow.get('configs')[configKey];\n  const data = msg.payload;\n  const convertData = {};\n  for (const [key, value] of Object.entries(data)) {\n    if (configs[key] != undefined) {\n      const { dataType, dataEncode } = configs[key];\n      parser.dataEncode = dataEncode ?? DATA_ENCODING.ABCD;\n      // 执行解析并输出结果\n      convertData[key] = parser.readValue(\n        value,  // 输入buffer\n        dataType ?? DATA_TYPES.HEX,\n      );\n    }\n  }\n  msg.payload = convertData;\n  const lastDataCacheKey = configKey + '-lastData';\n  let lastData = flow.get(lastDataCacheKey);\n  if (lastData == undefined) {\n    lastData = flow.get(lastDataCacheKey, 'file');\n  }\n  if (lastData != undefined) {\n    const data = {};\n    const reportTypes = ['正常上报', '变化即上报', '差值过量上报'];\n    Object.keys(convertData).forEach(key => {\n      const currentValue = convertData[key];\n      const historyValue = lastData[key];\n      if (reportTypes[configs[key].reportSetting] == '正常上报') {\n        data[key] = currentValue;\n      } else if (reportTypes[configs[key].reportSetting] == '变化即上报') {\n        if (currentValue !== historyValue) {\n          data[key] = currentValue;\n        }\n      } else if (reportTypes[configs[key].reportSetting] == '差值过量上报') {\n        if (Math.abs(currentValue - historyValue) >= configs[key].differenceThreshold) {\n          data[key] = currentValue;\n        }\n      }\n    });\n    msg.payload = data;\n  }\n  /** 更新缓存的最新数据 */\n  flow.set(lastDataCacheKey, convertData);\n  if (flow.get('reportAtBreakPoint')){\n    flow.set(lastDataCacheKey, convertData, 'file');\n  }\n  /** 先将需上报数据暂存 */\n  let reportDataCacheKey = configKey + '-reportData';\n  const reportData = flow.get(reportDataCacheKey) ?? [];\n  reportData.push({\n    time: Date.now(),\n    data: msg.payload,\n  })\n  flow.set(reportDataCacheKey, reportData);\n  if (flow.get('reportAtBreakPoint')) {\n    flow.set(reportDataCacheKey, reportData, 'file');\n  }\n  return msg;\n} catch (err) {\n  node.error(\"解析失败：\" + err.message, msg);\n  return null;\n}";
          return this.getRunFunctionNode(writeId, configKey, funcScript, "dataParseAndFilter");
        }

        getDataMergeNode(writeId, configKey) {
          const funcScript =
            "const configKey = msg.topic.configKey;\nconst countKey = configKey+'-count';\nlet count = flow.get(countKey);\ncount++;\nflow.set(countKey, count);\nif (!msg.responseBuffer || !msg.responseBuffer.buffer) {\n    node.warn(\"收到非数据消息，已忽略。消息内容: \" + JSON.stringify(msg.payload || msg));\n    return null;\n}\nconst currentData = flow.get(configKey + '-currentData') ?? {};\nconst currentConfig = flow.get('configs')[configKey];\n// 2. 按寄存器地址切分 Buffer\nconst buffer = Buffer.from(msg.responseBuffer.buffer);\nconst arrayBuffer = new ArrayBuffer(buffer.length);\nconst uint8Array = new Uint8Array(arrayBuffer);\nfor (let i = 0; i < buffer.length; i++) {\n    uint8Array[i] = buffer[i];\n}\ncurrentData[msg.topic.dataIden] = arrayBuffer;\nif (count >= flow.get(configKey+'-total')){\n    return { 'payload': currentData, 'configKey': configKey};\n} else {\n    flow.set(configKey + '-currentData', currentData);\n    return null;\n}";
          return this.getRunFunctionNode(writeId, configKey, funcScript, "dataMerge");
        }

        getRequestCreateNode(writeId, configKey) {
          const funcScript =
            "if (flow.get('configs') == undefined || typeof flow.get('configs') == 'string'){\n    const configs = flow.get('configs', 'file');\n    if(configs == undefined){\n      node.log('获取配置为空');\n      return null;   \n    } else {\n      flow.set('configs', configs);\n    }\n    flow.set(msg.configKey + '-messages', undefined);\n    node.log('重新加载配置');\n}\n/** 重置计数器 */\nflow.set(msg.configKey + '-count', 0);\n// 生成消息数组\nconst messages = flow.get(msg.configKey + '-messages') ?? [];\nif (flow.get(msg.configKey + '-messages') != undefined) {\n    return [flow.get(msg.configKey + '-messages')];\n} else {\n    const config = flow.get('configs')[msg.configKey];\n    if (config && Object.keys(config).length > 0) {\n        let total = 0;\n        const messages = [];\n        for (const [key, value] of Object.entries(config)) {\n            if(value['ioType'] == '只写'){\n                continue;\n            }\n            total++;\n            messages.push({\n                topic: {\n                    dataIden: key,\n                    configKey: msg.configKey\n                },\n                payload: value\n            });\n        }\n        flow.set(msg.configKey+'-total', total);\n        flow.set(msg.configKey + '-messages', messages);\n        return [messages];\n    } else {\n        return null;\n    }\n}";
          return this.getRunFunctionNode(writeId, configKey, funcScript, "requestCreate");
        }

        getCreateWriteRequestNode(writeId, configKey) {
          const funcScript =
            "const iconv = global.get('iconv');\nconst COIL_STATUS_AREA = 1;\nconst INPUT_STATUS_AREA = 2;\nconst HOLDING_REGISTER_AREA = 3;\nconst INPUT_REGISTER_AREA = 4;\n\nconst MAX_UINT16 = 65535;\nconst MIN_UINT16 = 0;\nconst MAX_INT16 = 32767;\nconst MIN_INT16 = -32768;\nconst MAX_UINT32 = 4294967295;\nconst MIN_UINT32 = 0;\nconst MAX_INT32 = 2147483647;\nconst MIN_INT32 = -2147483648;\nconst MAX_INT64 = 9223372036854775807n;\nconst MIN_INT64 = -9223372036854775808n;\nconst MAX_UNIT64 = 18446744073709551615n;\nconst MIN_UNIT64 = 0;\n\nconst DATA_TYPE_INT16 = 1;\nconst DATA_TYPE_UINT16 = 2;\nconst DATA_TYPE_INT32 = 3;\nconst DATA_TYPE_UINT32 = 4;\nconst DATA_TYPE_INT64 = 5;\nconst DATA_TYPE_FLOAT32 = 6;\nconst DATA_TYPE_FLOAT64 = 7;\nconst DATA_TYPE_BOOL = 8;\nconst DATA_TYPE_UTF8_ = 9;\nconst DATA_TYPE_BYTE = 10;\nconst DATA_TYPE_UNIT64 = 11;\nconst DATA_TYPE_GBK_ = 12;\n\nconst DATA_ENCODE_ABCD = 1;\nconst DATA_ENCODE_BADC = 2;\nconst DATA_ENCODE_CDAB = 3;\nconst DATA_ENCODE_DCBA = 4;\n\nconst TRANSFER_MODE_RTU = 'RTU';\nconst TRANSFER_MODE_ASCII = 'ASCII';\n\nif (flow.get('valueParser') == undefined || Object.keys(flow.get('valueParser')).length == 0) {\n    flow.set('valueParser', {\n        fc: undefined,\n        dataType: undefined,\n        dataEncode: undefined,\n        dataIden: undefined,\n        builderWriteRequestData: function (value) {\n            if (COIL_STATUS_AREA == this.fc) {\n                if (value != 0 && value != 1) {\n                    throw new Error('数值超出范围0-1');\n                }\n                return value;\n            } else if (HOLDING_REGISTER_AREA == this.fc) {\n                if ([DATA_TYPE_BYTE, DATA_TYPE_UINT16, DATA_TYPE_INT16].includes(this.dataType)) {\n                    let v = Number.parseInt(value);\n                    let maxRange = MAX_UINT16;\n                    let minRange = MIN_UINT16;\n                    if (this.dataType == DATA_TYPE_INT16) {\n                        maxRange = MAX_INT16;\n                        minRange = MIN_INT16;\n                    }\n                    if (maxRange < v || minRange > v) {\n                        throw new Error(this.dataIden + ':超出范围' + minRange + '-' + maxRange);\n                    }\n                    if ([DATA_TYPE_UINT16, DATA_TYPE_INT16]) {\n                        if (this.dataEncode == DATA_ENCODE_BADC) {\n                            v = ((((v & 0xff) << 8) & 0xff00) | ((v >> 8) & 0xff)) & 0xffff;\n                        } else if (this.dataEncode != DATA_ENCODE_ABCD) {\n                            throw new Error(this.dataIden + ':不支持的数据字节序');\n                        }\n                    }\n                    return v;\n                } else {\n                    let data;\n                    if (this.dataType == DATA_TYPE_UINT32 || this.dataType == DATA_TYPE_INT32) {\n                        const v = Number.parseInt(value);\n                        let minRange = MIN_UINT32;\n                        let maxRange = MAX_UINT32;\n                        if (this.dataType == DATA_TYPE_INT32) {\n                            minRange = MIN_INT32;\n                            maxRange = MAX_INT32;\n                        }\n                        if (maxRange < v || minRange > v) {\n                            throw new Error(this.dataIden + ':超出范围' + minRange + '-' + maxRange);\n                        }\n                        data = this._writeInt32(v, this.dataEncode);\n                    } else if (this.dataType == DATA_TYPE_FLOAT32) {\n                        const v = Number.parseFloat(value);\n                        data = this._writeFloat32(v, this.dataEncode);\n                    } else if (this.dataType == DATA_TYPE_INT64 || this.dataType == DATA_TYPE_UNIT64) {\n                        const v = BigInt(value);\n                        let minRange = MIN_UNIT64;\n                        let maxRange = MAX_UNIT64;\n                        if (this.dataType == DATA_TYPE_INT64) {\n                            minRange = MIN_INT64;\n                            maxRange = MAX_INT64;\n                        }\n                        if (maxRange < v || minRange > v) {\n                            throw new Error(this.dataIden + ':超出范围' + minRange + '-' + maxRange);\n                        }\n                        data = this._writeInt64(v, this.dataEncode);\n                    } else if (this.dataType == DATA_TYPE_FLOAT64) {\n                        const v = Number.parseFloat(value);\n                        data = this._writeFloat64(v, this.dataEncode);\n                    } else if ([DATA_TYPE_UTF8_, DATA_TYPE_GBK_].includes(this.dataType)) {\n                        const bytes = iconv.encode(value, DATA_TYPE_UTF8_ == this.dataType ? 'utf-8' : 'gbk');\n                        const paddedBytes = bytes.length % 2 === 1 ? Buffer.concat([bytes, Buffer.from([0])]) : bytes;\n                        const registers = [];\n                        const isLittleEndian = (this.dataEncode !== DATA_ENCODE_ABCD); // 假设 'ABCD' 是大端，否则小端\n                        for (let i = 0; i < paddedBytes.length; i += 2) {\n                            const high = paddedBytes[i];\n                            const low = paddedBytes[i + 1];\n                            if (isLittleEndian) {\n                                registers.push((low << 8) | high);\n                            } else {\n                                registers.push((high << 8) | low);\n                            }\n                        }\n                        data = new Uint16Array(registers);\n                    } else {\n                        throw new Error(this.dataIden + ':不支持设置的数据类型');\n                    }\n                    return data;\n                }\n            } else {\n                throw new Error('不支持写入的分区');\n            }\n        },\n        _writeInt32: function (value, encode) {\n            const data = [];\n            let valueHigh = (value >> 16) & 0xffff;\n            let valueLow = value & 0xffff;\n            switch (encode) {\n                case DATA_ENCODE_ABCD:\n                    data.push(valueHigh, valueLow);\n                    break;\n                case DATA_ENCODE_BADC:\n                    data.push(\n                        ((valueHigh << 8) & 0xff00) | ((valueHigh >> 8) & 0x00ff),\n                        ((valueLow << 8) & 0xff00) | ((valueLow >> 8) & 0x00ff)\n                    );\n                    break;\n                case DATA_ENCODE_CDAB:\n                    data.push(valueLow, valueHigh);\n                    break;\n                case DATA_ENCODE_DCBA:\n                    data.push(\n                        ((valueLow << 8) & 0xff00) | ((valueLow >> 8) & 0x00ff),\n                        ((valueHigh << 8) & 0xff00) | ((valueHigh >> 8) & 0x00ff)\n                    );\n                    break;\n                default:\n                    throw new Error('不支持的数据字节序');\n            }\n            return data;\n        },\n\n        _writeFloat32: function (value, encode) {\n            const float32 = new Float32Array([value]);\n            const int32 = new Int32Array(float32.buffer);\n            return this._writeInt32(int32[0], encode);\n        },\n\n        _writeInt64: function (value, encode) {\n            const valueHigh = (value >> 32n) & 0xffffffffn;\n            const valueLow = value & 0xffffffffn;\n            return encode == DATA_ENCODE_CDAB || encode == DATA_ENCODE_DCBA\n                ? [...this._writeInt32(Number(valueLow), encode), ...this._writeInt32(Number(valueHigh), encode)]\n                : [...this._writeInt32(Number(valueHigh), encode), ...this._writeInt32(Number(valueLow), encode)];\n        },\n\n        _writeFloat64: function (value, encode) {\n            const float64 = new Float64Array([value]);\n            const int64 = new BigInt64Array(float64.buffer);\n            return this._writeInt64(int64[0], encode);\n        },\n        getWriteValue: function (value, quantity) {\n            const reqData = this.builderWriteRequestData(value);\n            if (COIL_STATUS_AREA == this.fc) {\n                return { parseVal: reqData, fc: 5 };\n            } else if (HOLDING_REGISTER_AREA == this.fc) {\n                return { parseVal: reqData, fc: quantity > 1 ? 16 : 6 };\n            } else {\n                throw new Error('不支持写入的分区');\n            }\n        },\n    });\n}\n\nif (flow.get('configs') == undefined) {\n    const configs = flow.get('configs', 'file');\n    if (configs == undefined) {\n        return null;\n    } else {\n        flow.set('configs', configs);\n    }\n}\n\ntry {\n    const configs = flow.get('configs');\n    if (configs == undefined) {\n        return null;\n    }\n    const requestMap = new Map();\n    const parser = flow.get('valueParser');\n    for (const [key, value] of Object.entries(msg.payload)) {\n        for (const [configKey, item] of Object.entries(configs)) {\n            if (item[key] != undefined && item[key]['ioType'] != '只读') {\n                const serialNumber = configKey.split('-')[2];\n                let req;\n                if (item[key]['fc'] != undefined) {\n                    parser.fc = item[key].fc;\n                    parser.dataType = item[key].dataType;\n                    parser.dataEncode = item[key].dataEncode;\n                    parser.dataIden = key;\n                    const { parseVal, fc } = parser.getWriteValue(value, item[key].quantity);\n                    req = {\n                        ...item[key],\n                        value: parseVal\n                    };\n                    req.fc = fc;\n                } else {\n                    req = {\n                        payload: value,\n                        variable: key\n                    };\n                }\n                if (requestMap.has(serialNumber)) {\n                    requestMap.get(serialNumber).push(req)\n                } else {\n                    requestMap.set(serialNumber, [req]);\n                }\n                break;\n            }\n        }\n    }\n    const data = [];\n    for (const [key, value] of requestMap.entries()) {\n        data.push({\n            serialNumber: key,\n            requests: value,\n        })\n    }\n    msg.payload = data;\n    return msg;\n} catch (error) {\n    node.error(error.message, msg);\n    return null;\n}";
          return this.getRunFunctionNode(writeId, configKey, funcScript, "createWriteRequest");
        }

        setWriesArray(objectNode, writeIds) {
          const wires = [];
          for (const id of writeIds) {
            const wireArray = [id];
            wires.push(wireArray);
          }
          objectNode.wires = wires;
        }

        getModbusNode(endpoint) {
          return {
            id: endpoint,
            type: "modbus-client",
            tcpPort: "502",
            bufferCommands: true,
            stateLogEnabled: false,
            queueLogEnabled: false,
            failureLogEnabled: true,
            tcpType: "DEFAULT",
            serialPort: "/dev/ttyUSB",
            serialType: "RTU-BUFFERD",
            serialBaudrate: "9600",
            serialDatabits: "8",
            serialStopbits: "1",
            serialParity: "none",
            serialConnectionDelay: "100",
            serialAsciiResponseStartDelimiter: "0x3A",
            unit_id: 1,
            commandDelay: 1,
            reconnectOnTimeout: true,
            reconnectTimeout: 2000,
            parallelUnitIdsAllowed: true,
            showErrors: false,
            showWarnings: true,
            showLogs: true
          };
        }

        getGetterNode(writeId, endpoint, name) {
          const getterNode = {
            id: `${endpoint}-getter`,
            name: name,
            type: "modbus-flex-getter",
            showStatusActivities: true,
            showErrors: true,
            showWarnings: true,
            logIOActivities: false,
            dataType: "HoldingRegister",
            server: endpoint,
            useIOFile: false,
            ioFile: "",
            useIOForPayload: false,
            emptyMsgOnFail: true,
            keepMsgProperties: true,
            delayOnStart: false,
            startDelayTime: ""
          };

          this.setWriesArray(getterNode, [writeId]);
          return getterNode;
        }

        getWriteNode(endpoint) {
          return {
            id: `${endpoint}-write`,
            type: "modbus-flex-write",
            name: endpoint,
            showStatusActivities: true,
            showErrors: true,
            showWarnings: true,
            server: endpoint,
            emptyMsgOnFail: true,
            keepMsgProperties: true,
            delayOnStart: false,
            startDelayTime: ""
          };
        }

        getProps(fieldName, value, valueType) {
          const propsArray = [];
          const payloadProp = {
            p: fieldName,
            v: value,
            vt: valueType
          };
          propsArray.push(payloadProp);
          return propsArray;
        }

        getInjectNode(writeId, cycleTime, props, name) {
          const injectNode = {
            id: name == undefined ? `${writeId}-inject` : `${writeId}-${name}`,
            type: "inject",
            name: name == undefined ? "" : name,
            props: props,
            repeat: cycleTime != undefined ? (cycleTime / 1000).toFixed(2) : "",
            crontab: "",
            once: cycleTime == undefined,
            onceDelay: "1",
            topic: ""
          };

          this.setWriesArray(injectNode, [writeId]);
          return injectNode;
        }

        getSplitNode(writeId, configKey) {
          const objectNode = {
            id: `${configKey}-split`,
            name: "",
            type: "split",
            splt: "\\n",
            spltType: "str",
            arraySplt: 1,
            arraySpltType: "len",
            stream: false,
            addname: "",
            property: "payload"
          };

          this.setWriesArray(objectNode, [writeId]);
          return objectNode;
        }

        getSwitchNode(writeIds, configKey, serialNumbers) {
          const rules = serialNumbers.map(num => ({
            t: "eq",
            v: num,
            vt: "str"
          }));
          node.log("\nswitchNode serialNumbers:" + JSON.stringify(serialNumbers))
          node.log("\nswitchNode rules:" + JSON.stringify(rules))

          const objectNode = {
            id: `${configKey}-switch`,
            type: "switch",
            property: "payload.serialNumber",
            propertyType: "msg",
            rules: rules,
            checkall: "true",
            repair: false,
            outputs: writeIds.length
          };

          this.setWriesArray(objectNode, writeIds);
          return objectNode;
        }

        getRequestFormatNode(writeId, endpoint) {
          const funcScript = "const requests = msg.payload.requests.map(req => {\n    return {\n        topic: `${req.unitid}-${req.address}`,\n        payload: {\n            address: req.address,\n            value: req.value,\n            unitid: req.unitid,\n            fc: req.fc,\n            quantity: req.quantity\n        }\n    };\n});\nreturn [requests];";
          return this.getRunFunctionNode(writeId, endpoint, funcScript, "requestFormat");
        }

        getNetworkRequestFormatNode(writeId, endpoint) {
          const funcScript = "const requests = msg.payload.requests.map(req => {\n    return {\n        topic: req.variable,\n        ...req,\n    };\n});\nreturn [requests];";

          return this.getRunFunctionNode(writeId, endpoint, funcScript, "networkRequestFormat");
        }

        getRunFunctionNode(writeId, configKey, funcScript, name) {
          return this.getFunctionNode(writeId, configKey, funcScript, "", "", name);
        }
        getFinalizeNode(configKey, finalize, name) {
          return this.getFunctionNode(undefined, configKey, "", "", finalize, name);
        }

        getFunctionNode(writeId, configKey, funcScript, initialize, finalize, name) {
          const objectNode = {
            id: `${configKey}-${name}`,
            type: "function",
            name: name,
            func: funcScript,
            outputs: 1,
            timeout: 0,
            noerr: 0,
            initialize: initialize,
            finalize: finalize,
            libs: []
          };
          if (writeId != undefined) {
            this.setWriesArray(objectNode, [writeId]);
          }
          return objectNode;
        }
      }

      const updateFlows = async (flows, topic) => {
        const response = await axios.post('http://localhost:1880/admin/flows', {
          flows: flows,
          // credentials: flowConfig.credentials
        }, {
          headers: {
            // 'Authorization': `Bearer ${apiKey}`,
            'Content-Type': 'application/json',
            'Node-RED-API-Version': 'v2',
            'Node-RED-Deployment-Type': 'nodes',
          }
        });
        node.log("返回结果: " + response.data);
        if (response.status === 200) {
          node.send({
            message: '流程更新成功',
            data: flows,
          })
          return true;
        } else {
          node.send({
            message: '流程更新失败',
            data: flows,
          })
          return false;
        };
      }
      /** 部署新流程 */
      const deployFlows = async (nodeConfigs, replaceAll, topic) => {
        try {
          const flowsCreateutil = new FlowsCreateUtil();
          const flows = flowsCreateutil.getFlows(nodeConfigs);
          if (replaceAll) {
            return await updateFlows(flows, topic);
          } else {
            return await axios.get('http://localhost:1880/admin/flows', {
              headers: {
                // 'Authorization': `Bearer ${apiKey}`,
                'Content-Type': 'application/json',
                'Node-RED-API-Version': 'v2',
                'Node-RED-Deployment-Type': 'nodes',
              }
            }).then(async (res) => {
              node.log("获取流程数据:" + JSON.stringify(res.data));
              const keepNodes = ['tab', 'wulink-config', 'wulink-in', 'wulink-out', 'flow-update'];
              const oldNodes = res.data.flows.filter(a => keepNodes.includes(a.type));
              const nodeMap = new Map();
              for (const item of oldNodes) {
                nodeMap.set(item.type, item);
              }
              nodeMap.get('wulink-in').wires = [[]];
              for (const item of flows) {
                item["z"] = nodeMap.get('tab').id;
                if (item.needReport) {
                  if (item.wires == undefined) {
                    item.wires = [[]];
                  }
                  item.wires[0].push(nodeMap.get('wulink-out').id);
                } else if (item.needWrite) {
                  nodeMap.get('wulink-in').wires[0].push(item.id);
                }
              }
              oldNodes.push(...flows);
              node.log("更新后的数据:" + JSON.stringify(oldNodes));
              return await updateFlows(oldNodes);
            })
          }
        } catch (err) {
          node.error(`部署失败: ${err}`);
          return false;
        }
      }


      // 处理配置消息
      mqttClient.on('message', async (topic, message) => {
        if (topic === mqTopic) {
          try {
            node.log("收到配置数据: " + message);
            const payload = JSON.parse(message);
            const data = payload.data;
            const success = await deployFlows(data.nodeConfigs, data.replaceAll, topic);
            node.log('流程部署返回结果: ' + success);
            if (success) {
              const keys = node.context().flow.keys();
              // 部署成功后应清空上下文缓存
              keys.forEach(key => {
                node.context().flow.set(key, undefined);
              });
              node.context().flow.set('configs', data.channelConfigs, 'file');
              node.context().flow.set('reportAtBreakPoint', data.nodeConfigs.reportAtBreakPoint);
              // 立刻从文件读回并打印
              const saved = node.context().flow.get('configs', 'file');
              /* 触发配置缓存更新 */
              node.context().flow.set('configs', undefined);
              node.log("通道配置更新成功");
              mqttClient.publish(topic + '_reply', JSON.stringify({
                id: payload.id,
                method: payload.method + "_reply",
                version: "1.0",
                code: 20000,
                message: "配置更新成功"
              }), { qos: 1 });
            } else {
              mqttClient.publish(topic + '_reply', JSON.stringify({
                id: payload.id,
                method: payload.method + "_reply",
                version: "1.0",
                code: 40000,
                message: "配置更新失败"
              }), { qos: 1 });
            }
          } catch (err) {
            node.error("处理消息时出错:" + err.message);
          }
        } else if (topic === otaTopic) {
          node.log("收到OTA数据: " + message);
          const data = JSON.parse(message);
          node.send(JSON.parse(message));
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
  RED.nodes.registerType("flow-update", FlowUpdateNode);
};