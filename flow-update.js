const crypto = require('crypto');
const axios = require('axios');
const mqtt = require('mqtt');
const fs = require('fs');
const path = require('path');

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

      // 订阅配置Topic
      mqttClient.on('connect', () => {
        node.status({ fill: 'green', shape: 'dot', text: '已连接' });
        mqttClient.subscribe(mqTopic, { qos: 0 }, (err) => {
          if (!err) node.log(`已订阅配置下发Topic: ${mqTopic}`);
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
            const saveReportDataNode = this.getSaveReportDataNode(configData.id);
            const reportAndClearDataNode = this.getReportAndClearDataNode(configData.id);
            reportAndClearDataNode.needReport = true;
            const reportInjectNode = this.getInjectNode(
              reportAndClearDataNode.id,
              configData.reportCycle,
            );

            flows.push(saveReportDataNode);
            flows.push(reportAndClearDataNode);
            flows.push(reportInjectNode);
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
                connmode: PROTOCOL_TYPES[config.protocol] == 'smart-200' ? "rack-slot" : "tsap",
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
              objectNode.serialPort = "COM2"; // 可根据实际需要调整
              objectNode.tcpType = "DEFAULT";
              objectNode.tcpHost = "127.0.0.1";

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
          objectNode.tcpType = "UDP";
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
            "if (flow.get('configs') == undefined){\n  const configs = flow.get('configs', 'file');\n    if (configs == undefined){\n      node.log('获取配置为空');\n      return null;   \n    } else {\n    flow.set('configs', configs);\n    }\n    node.log('重新加载上报设置配置');\n}\nconst reportTypes = ['正常上报', '变化即上报', '差值过量上报'];\nlet data = msg.payload;\nconst configKey = node.id.substring(0, node.id.length - \"filterAndCacheData\".length - 1);\nconst reportSetting = flow.get('configs')[configKey];\nconst lastDataCacheKey = configKey + '-lastData';\nlet lastData = flow.get(lastDataCacheKey);\nif (lastData == undefined && flow.get('lastDataMap', 'file') != undefined){\n  lastData = flow.get('lastDataMap', 'file')[lastDataCacheKey];\n}\nif (lastData != undefined) {\n  const filterData = {};\n    Object.keys(data).forEach(key => {\n      const currentValue = data[key];\n      const historyValue = lastData[key];\n      if (reportTypes[reportSetting[key].reportSetting] == '正常上报') {\n        filterData[key] = currentValue;\n      } else if (reportTypes[reportSetting[key].reportSetting] == '变化即上报'){\n        if (currentValue !== historyValue) {\n          filterData[key] = currentValue;\n        }\n      } else {\n        if (Math.abs(currentValue - historyValue) >= reportSetting[key].differenceThreshold) {\n          filterData[key] = currentValue;\n        }\n      }\n  });\n  data = filterData;\n}\n/** 更新缓存的最新数据 */\nflow.set(lastDataCacheKey, msg.payload);\nconst reportDataCacheKey = node.id + '-reportData';\nconst reportData = flow.get(reportDataCacheKey) ?? [];\nreportData.push({\n    time: Date.now(),\n    data: data,\n})\nnode.log('s7-smrt缓存数据:' + JSON.stringify(data));\nflow.set(reportDataCacheKey, reportData);\nreturn msg;";
          return this.getRunFunctionNode(undefined, configKey, funScript, "filterAndCacheData");
        }

        getSaveReportDataNode(configKey) {
          const funcScript =
            "try {\n    const reportAtBreakPoint = flow.get('reportAtBreakPoint', 'file') ?? true;\n    if(reportAtBreakPoint){\n        const saveReportData = flow.get('reportData', 'file') ?? [];\n        const lastDataMap = flow.get('lastDataMap', 'file') ?? {};\n        const keys = flow.keys();\n        for (const key of keys) {\n            if (key.endsWith('-reportData')) {\n                const reportData = flow.get(key) ?? [];\n                saveReportData.push(...reportData);\n                node.log('reportData:' + JSON.stringify(reportData))\n                flow.set(key, null);\n            }\n            if (key.endsWith('-lastData')) {\n                const lastData = flow.get(key) ?? {};\n                lastDataMap[key] = lastData;\n                node.log('lastData:' + JSON.stringify(lastData))\n                flow.set(key, null);\n            }\n        }\n        node.log(\"saveReportData:\" + JSON.stringify(saveReportData));\n        node.log(\"lastDataMap:\" + JSON.stringify(lastDataMap));\n        flow.set('lastDataMap', lastDataMap, 'file');\n        flow.set('reportData', saveReportData, 'file');\n        node.log('数据持久化完成');\n    }\n} catch (err) {\n    node.error('数据持久化失败: ' + err.message);\n}";
          return this.getFinalizeNode(configKey, funcScript, "saveReportData");
        }

        getReportAndClearDataNode(configKey) {
          const funcScript =
            "try {\n    node.log('上报已采集的数据');\n    const saveReportData = flow.get('reportData', 'file') ?? [];\n    for (const key of flow.keys()){\n        if (key.endsWith('-reportData')){\n            const reportData = flow.get(key) ?? [];\n            saveReportData.push(...reportData);\n            flow.set(key, []);       \n        }\n    }\n    const timeMap = new Map();\n    for (const item of saveReportData) {\n        if (timeMap.has(item.time)) {\n            timeMap.set(item.time, { ...timeMap.get(item.time), ...item.data });\n        } else {\n            timeMap.set(item.time, item.data);\n        }\n    }\n    const sortedEntries = [...timeMap].sort((a, b) => a[0] - b[0]);\n    const sortedReportData = sortedEntries.map(entry => {\n        return {\n            time: entry[0],\n            payload: entry[1]\n        }\n    });\n    flow.set('reportData', null, 'file');\n    node.send({ ...msg, type: 'batchProperty', payload: sortedReportData });\n    return null;\n} catch (err) {\n    node.error('上报数据持久化失败: ' + err.message);\n    return null;\n}";
          return this.getRunFunctionNode(undefined, configKey, funcScript, "reportAndClearData");
        }

        getDataParserAndFilterNode(writeId, configKey) {
          const funcScript =
            "const DATA_TYPES = {\n  HEX: 0, INT16: 1, UINT16: 2, INT32: 3, UINT32: 4, INT64: 5,\n  FLOAT32: 6, FLOAT64: 7, BIT: 8, UTF8: 9, BOOL: 10,\n  UINT64: 11, GBK: 12\n};\nconst TRANSFER_MODE_RTU = 'RTU';\nconst DATA_ENCODING = {\n  ABCD: 1, BADC: 2,\n  CDAB: 3, DCBA: 4\n};\n\n\nconst iconv = global.get('iconv');\n\nif (!global.get('bufferParser')) {\n  global.set('bufferParser', {\n    // 十进制转换（支持跨节点复用）\n    dataEncode: DATA_ENCODING.ABCD,\n    decimalPlaces: 0,\n    convertToRealValue: function (value) {\n      if (this.decimalPlaces == undefined || this.decimalPlaces == 0) {\n        return value;\n      }\n      return value / Math.pow(10, this.decimalPlaces);\n    },\n    // 核心解析逻辑（包含字节序处理）\n    readValue: function (buffer, dataType) {\n      node.log('readValue buffer:' + new Uint8Array(buffer));\n      const parser = this;\n      switch (dataType) {\n        case DATA_TYPES.HEX:\n          return parser._readHex(buffer);\n        case DATA_TYPES.BIT:\n          return parser._readBit(buffer);\n        case DATA_TYPES.UINT16:\n          return parser.convertToRealValue(parser._readUint16(buffer));\n        case DATA_TYPES.INT16:\n          return parser.convertToRealValue(parser._readInt16(buffer));\n        case DATA_TYPES.UINT32:\n          return parser.convertToRealValue(parser._readUint32(buffer));\n        case DATA_TYPES.INT32:\n          return parser.convertToRealValue(parser._readInt32(buffer));\n        case DATA_TYPES.FLOAT32:\n          return parser._readFloat32(buffer);\n        case DATA_TYPES.INT64:\n          return parser.convertToRealValue(parser._readInt64(buffer));\n        case DATA_TYPES.UINT64:\n          return parser.convertToRealValue(parser._readUInt64(buffer))\n        case DATA_TYPES.FLOAT64:\n          return parser._readFloat64(buffer);\n        case DATA_TYPES.UTF8:\n          return parser._read(buffer, 'utf-8');\n        case DATA_TYPES.GBK:\n          return parser._read(buffer, 'gbk');\n        default:\n          throw new Error('不支持的 dataType:' + dataType);\n      }\n    },\n\n    // 具体解析方法（私有方法前缀_）\n    _readHex: function (buffer) {\n      return Array.from(new Uint8Array(buffer),\n        byte => ('0' + byte.to(16).toUpperCase()).slice(-2)).join(' ');\n    },\n\n    _readBit: function (buffer) {\n      node.log(\"_readBit: \" + new Uint8Array(buffer));\n      return new DataView(buffer).getUint8(0) & 0x01;\n    },\n\n    _readUint16: function (buffer) {\n      const dv = new DataView(buffer);\n      switch (this.dataEncode) {\n        case DATA_ENCODING.ABCD: return dv.getUint16(0);\n        case DATA_ENCODING.BADC: return dv.getUint16(0, true);\n        default:\n          throw new Error('不支持的字节序:' + this.dataEncode);\n      }\n    },\n    _readInt16: function (buffer) {\n      const dv = new DataView(buffer);\n      switch (this.dataEncode) {\n        case DATA_ENCODING.ABCD:\n          return dv.getInt16(0);\n        case DATA_ENCODING.BADC:\n          return dv.getInt16(0, true);\n        default:\n          throw new Error('不支持的数据字节序');\n      }\n    },\n\n    _readInt32: function (buffer) {\n      const dv = new DataView(buffer);\n      switch (this.dataEncode) {\n        case DATA_ENCODING.ABCD:\n          return dv.getInt32(0);\n        case DATA_ENCODING.BADC:\n          return (dv.getInt16(0, true) << 16) | dv.getInt16(2, true);\n        case DATA_ENCODING.CDAB:\n          return dv.getInt16(0) | (dv.getInt16(2) << 16);\n        case DATA_ENCODING.DCBA:\n          return dv.getInt32(0, true);\n        default:\n          throw new Error('不支持的数据字节序');\n      }\n    },\n\n    _readUint32: function (buffer) {\n      return this._readInt32(buffer) >>> 0;\n    },\n\n    _readFloat32: function (buffer) {\n      const int32 = new Int32Array([this._readInt32(buffer)]);\n      return new Float32Array(int32.buffer)[0];\n    },\n\n    _readInt64: function (buffer) {\n      const dv = new DataView(buffer);\n      switch (this.dataEncode) {\n        case DATA_ENCODING.ABCD:\n          return dv.getBigInt64(0);\n        case DATA_ENCODING.BADC:\n          const bufBadc = new Uint8Array([\n            dv.getUint8(1),\n            dv.getUint8(0),\n            dv.getUint8(3),\n            dv.getUint8(2),\n            dv.getUint8(5),\n            dv.getUint8(4),\n            dv.getUint8(7),\n            dv.getUint8(6),\n          ]);\n          return new DataView(bufBadc.buffer).getBigInt64(0);\n        case DATA_ENCODING.CDAB:\n          const bufCdab = new Uint8Array([\n            dv.getUint8(6),\n            dv.getUint8(7),\n            dv.getUint8(4),\n            dv.getUint8(5),\n            dv.getUint8(2),\n            dv.getUint8(3),\n            dv.getUint8(0),\n            dv.getUint8(1),\n          ]);\n          return new DataView(bufCdab.buffer).getBigInt64(0);\n        case DATA_ENCODING.DCBA:\n          return dv.getBigInt64(0, true);\n        default:\n          throw new Error('不支持的数据字节序');\n      }\n    },\n\n    _readUint64: function (buffer) {\n      return this._readInt64(buffer) >>> 0;\n    },\n\n    _readFloat64: function (buffer) {\n      const dv = new DataView(buffer);\n      switch (this.dataEncode) {\n        case DATA_ENCODING.ABCD:\n          return dv.getFloat64(0);\n        case DATA_ENCODING.BADC:\n          const bufBadc = new Uint8Array([\n            dv.getUint8(1),\n            dv.getUint8(0),\n            dv.getUint8(3),\n            dv.getUint8(2),\n            dv.getUint8(5),\n            dv.getUint8(4),\n            dv.getUint8(7),\n            dv.getUint8(6),\n          ]);\n          return new DataView(bufBadc.buffer).getFloat64(0);\n        case DATA_ENCODING.CDAB:\n          const bufCdab = new Uint8Array([\n            dv.getUint8(6),\n            dv.getUint8(7),\n            dv.getUint8(4),\n            dv.getUint8(5),\n            dv.getUint8(2),\n            dv.getUint8(3),\n            dv.getUint8(0),\n            dv.getUint8(1),\n          ]);\n          return new DataView(bufCdab.buffer).getFloat64(0);\n        case DATA_ENCODING.DCBA:\n          return dv.getFloat64(0, true);\n        default:\n          throw new Error('不支持的数据字节序');\n      }\n    },\n\n    _read: function (buffer, encodeType) {\n      node.log('编码: ' + encodeType);\n      let str = iconv.decode(buffer, encodeType);\n      node.log('read:' + str);\n      for (let i = str.length - 1; i >= 0; i--) {\n        if (str[i].codePointAt() == 0) {\n          continue;\n        }\n        str = str.slice(0, Math.max(0, i + 1));\n        break;\n      }\n      return str;\n    }\n  });\n}\n\n// 主处理逻辑\nconst parser = global.get('bufferParser');\ntry {\n  node.log(\"进入dataParse方法\");\n  const configKey = msg.configKey;\n  const configs = flow.get('configs')[configKey];\n  const data = msg.payload;\n  const convertData = {};\n  node.log(\"原始数据: \" + JSON.stringify(data));\n  for (const [key, value] of Object.entries(data)) {\n    if (configs[key] != undefined) {\n      const { dataType, dataEncode, decimalPlaces } = configs[key];\n      node.log(\"通道配置: \" + JSON.stringify(configs[key]));\n      node.log(\"buffer: \" + new Uint8Array(value));\n      parser.dataEncode = dataEncode ?? DATA_ENCODING.ABCD;\n      parser.decimalPlaces = decimalPlaces ?? 0;\n      // 执行解析并输出结果\n      convertData[key] = parser.readValue(\n        value,  // 输入buffer\n        dataType ?? DATA_TYPES.HEX,\n      );\n      node.log(key+'转换后的值:'+convertData[key]);\n    }\n  }\n  msg.payload = convertData;\n  const lastDataCacheKey = configKey + '-lastData';\n  let lastData = flow.get(lastDataCacheKey);\n  if (lastData == undefined && flow.get('lastDataMap', 'file') != undefined){\n    lastData = flow.get('lastDataMap', 'file')[lastDataCacheKey];\n  }\n  if (lastData != undefined) {\n    const data = {};\n    const reportTypes = ['正常上报', '变化即上报', '差值过量上报'];\n    Object.keys(convertData).forEach(key => {\n      const currentValue = convertData[key];\n      const historyValue = lastData[key];\n      if (reportTypes[configs[key].reportSetting] == '正常上报') {\n        data[key] = currentValue;\n      } else if (reportTypes[configs[key].reportSetting] == '变化即上报') {\n        if (currentValue !== historyValue) {\n          data[key] = currentValue;\n        }\n      } else if (reportTypes[configs[key].reportSetting] == '差值过量上报') {\n        if (Math.abs(currentValue - historyValue) >= configs[key].differenceThreshold) {\n          data[key] = currentValue;\n        }\n      }\n    });\n    msg.payload = data;\n  }\n  /** 更新缓存的最新数据 */\n  flow.set(lastDataCacheKey, convertData);\n  /** 先将需上报数据暂存 */\n  let reportDataCacheKey = configKey + '-reportData';\n  const reportData = flow.get(reportDataCacheKey) ?? [];\n  reportData.push({\n    time: Date.now(),\n    data: msg.payload,\n  })\n  node.log('缓存数据:' + JSON.stringify(msg.payload));\n  flow.set(reportDataCacheKey, reportData);\n  return msg;\n} catch (err) {\n  node.error(\"解析失败：\" + err.message, msg);\n  return null;\n}";
          return this.getRunFunctionNode(writeId, configKey, funcScript, "dataParseAndFilter");
        }

        getDataMergeNode(writeId, configKey) {
          const funcScript =
            "const configKey = msg.topic.configKey;\nconst currentData = flow.get(configKey + '-currentData') ?? {};\nconst currentConfig = flow.get('configs')[configKey];\nnode.log(\"dataMerge msg: \" + JSON.stringify(msg));\n// 2. 按寄存器地址切分 Buffer\nconst buffer = Buffer.from(msg.responseBuffer.buffer);\nconst arrayBuffer = new ArrayBuffer(buffer.length);\nconst uint8Array = new Uint8Array(arrayBuffer);\nfor (let i = 0; i < buffer.length; i++) {\n    uint8Array[i] = buffer[i];\n}\ncurrentData[msg.topic.dataIden] = arrayBuffer;\nconst countKey = configKey+'-count';\nlet count = flow.get(countKey);\ncount++;\nflow.set(countKey, count);\nnode.log('currentConfig: ' + JSON.stringify(currentConfig));\nnode.log('count: ' + count);\nnode.log('ssss:' + flow.get(configKey +'-total'));\nif (count >= flow.get(configKey+'-total')){\n    return { 'payload': currentData, 'configKey': configKey};\n} else {\n    flow.set(configKey + '-currentData', currentData);\n    return null;\n}";
          return this.getRunFunctionNode(writeId, configKey, funcScript, "dataMerge");
        }

        getRequestCreateNode(writeId, configKey) {
          const funcScript =
            "if (flow.get('configs') == undefined || typeof flow.get('configs') == 'string'){\n    const configs = flow.get('configs', 'file');\n    if(configs == undefined){\n      node.log('获取配置为空');\n      return null;   \n    } else {\n      flow.set('configs', configs);\n    }\n    flow.set(msg.configKey + '-messages', undefined);\n    node.log('重新加载配置');\n}\nnode.log('当前配置: ' + JSON.stringify(flow.get('configs')));\n/** 重置计数器 */\nflow.set(msg.configKey + '-count', 0);\n// 生成消息数组\nconst messages = flow.get(msg.configKey + '-messages') ?? [];\nif (flow.get(msg.configKey + '-messages') != undefined) {\n    return [flow.get(msg.configKey + '-messages')];\n} else {\n    const config = flow.get('configs')[msg.configKey];\n    if (config && Object.keys(config).length > 0) {\n        let total = 0;\n        const messages = [];\n        for (const [key, value] of Object.entries(config)) {\n            if(value['ioType'] == '只写'){\n                continue;\n            }\n            total++;\n            messages.push({\n                topic: {\n                    dataIden: key,\n                    configKey: msg.configKey\n                },\n                payload: value\n            });\n        }\n        flow.set(msg.configKey+'-total', total);\n        node.log('生成消息成功: ' + JSON.stringify(messages));\n        flow.set(msg.configKey + '-messages', messages);\n        return [messages];\n    } else {\n        return null;\n    }\n}";
          return this.getRunFunctionNode(writeId, configKey, funcScript, "requestCreate");
        }

        getCreateWriteRequestNode(writeId, configKey) {
          const funcScript =
            "const iconv = global.get('iconv');\nconst COIL_STATUS_AREA = 0;\nconst INPUT_STATUS_AREA = 1;\nconst HOLDING_REGISTER_AREA = 3;\nconst INPUT_REGISTER_AREA = 4;\n\nconst MAX_UINT16 = 65535;\nconst MIN_UINT16 = 0;\nconst MAX_INT16 = 32767;\nconst MIN_INT16 = -32768;\nconst MAX_UINT32 = 4294967295;\nconst MIN_UINT32 = 0;\nconst MAX_INT32 = 2147483647;\nconst MIN_INT32 = -2147483648;\nconst MAX_INT64 = 9223372036854775807n;\nconst MIN_INT64 = -9223372036854775808n;\nconst MAX_UNIT64 = 18446744073709551615n;\nconst MIN_UNIT64 = 0;\n\nconst DATA_TYPE_HEX = 0;\nconst DATA_TYPE_INT16 = 1;\nconst DATA_TYPE_UINT16 = 2;\nconst DATA_TYPE_INT32 = 3;\nconst DATA_TYPE_UINT32 = 4;\nconst DATA_TYPE_INT64 = 5;\nconst DATA_TYPE_FLOAT32 = 6;\nconst DATA_TYPE_FLOAT64 = 7;\nconst DATA_TYPE_BIT = 8;\nconst DATA_TYPE_UTF8_ = 9;\nconst DATA_TYPE_BOOL = 10;\nconst DATA_TYPE_UNIT64 = 11;\nconst DATA_TYPE_GBK_ = 12;\n\nconst DATA_ENCODE_ABCD = 1;\nconst DATA_ENCODE_BADC = 2;\nconst DATA_ENCODE_CDAB = 3;\nconst DATA_ENCODE_DCBA = 4;\n\nconst TRANSFER_MODE_RTU = 'RTU';\nconst TRANSFER_MODE_ASCII = 'ASCII';\n\nif (global.get('valueParser') == undefined || Object.keys(global.get('valueParser')).length == 0) {\n    global.set('valueParser', {\n        fc: undefined,\n        dataType: undefined,\n        dataEncode: undefined,\n        dataIden: undefined,\n        _getWordLength: function() {\n            return this.quantity / 2 + (this.quantity % 2 != 0 ? 1 : 0);\n        },\n        builderWriteRequestData: function (value) {\n            if (COIL_STATUS_AREA == this.fc) {\n                return value ? true : false;\n            } else if (HOLDING_REGISTER_AREA == this.fc) {\n                if ([DATA_TYPE_UINT16, DATA_TYPE_INT16, DATA_TYPE_HEX].includes(this.dataType)) {\n                    let v = this.dataType == DATA_TYPE_HEX ? Number.parseInt(value.replace(' ', ''), 16) : Number.parseInt(value);\n                    let maxRange = MAX_UINT16;\n                    let minRange = MIN_UINT16;\n                    if (this.dataType == DATA_TYPE_INT16) {\n                        maxRange = MAX_INT16;\n                        minRange = MIN_INT16;\n                    }\n                    if (maxRange < v || minRange > v) {\n                        throw new Error(this.dataIden + ':超出范围' + minRange + '-' + maxRange);\n                    }\n                    if (this.dataEncode == DATA_ENCODE_BADC) {\n                        v = ((((v & 0xff) << 8) & 0xff00) | ((v >> 8) & 0xff)) & 0xffff;\n                    } else if (this.dataEncode != DATA_ENCODE_ABCD) {\n                        throw new Error(this.dataIden + ':不支持的数据字节序');\n                    }\n                    return v;\n                } else {\n                    let data;\n                    if (this.dataType == DATA_TYPE_UINT32 || this.dataType == DATA_TYPE_INT32) {\n                        const v = Number.parseInt(value);\n                        let minRange = MIN_UINT32;\n                        let maxRange = MAX_UINT32;\n                        if (this.dataType == DATA_TYPE_INT32) {\n                            minRange = MIN_INT32;\n                            maxRange = MAX_INT32;\n                        }\n                        if (maxRange < v || minRange > v) {\n                            throw new Error(this.dataIden + ':超出范围' + minRange + '-' + maxRange);\n                        }\n                        data = this._writeInt32(v, this.dataEncode);\n                    } else if (this.dataType == DATA_TYPE_FLOAT32) {\n                        const v = Number.parseFloat(value);\n                        data = this._writeFloat32(v, this.dataEncode);\n                    } else if (this.dataType == DATA_TYPE_INT64 || this.dataType == DATA_TYPE_UNIT64) {\n                        const v = BigInt(value);\n                        let minRange = MIN_UNIT64;\n                        let maxRange = MAX_UNIT64;\n                        if (this.dataType == DATA_TYPE_INT64) {\n                            minRange = MIN_INT64;\n                            maxRange = MAX_INT64;\n                        }\n                        if (maxRange < v || minRange > v) {\n                            throw new Error(this.dataIden + ':超出范围' + minRange + '-' + maxRange);\n                        }\n                        data = this._writeInt64(v, this.dataEncode);\n                    } else if (this.dataType == DATA_TYPE_FLOAT64) {\n                        const v = Number.parseFloat(value);\n                        data = this._writeFloat64(v, this.dataEncode);\n                    } else if ([DATA_TYPE_UTF8_, DATA_TYPE_GBK_].includes(this.dataType)) {\n                        const bytes = iconv.encode(value, DATA_TYPE_UTF8_ == this.dataType ? 'utf-8' : 'gbk');\n                        const paddedBytes = bytes.length % 2 === 1 ? [...bytes, 0] : bytes;\n                        const size = paddedBytes.length;\n                        const v = new Uint8Array(size);\n                        for (let i = 0; i < size; i = i + 2) {\n                            v.set([paddedBytes[i + 1], paddedBytes[i]], i);\n                        }\n                        data = new Uint16Array(v.buffer);\n                    } else {\n                        throw new Error(this.dataIden + ':不支持设置的数据类型');\n                    }\n                    return data;\n                }\n            } else {\n                throw new Error('不支持写入的分区ddd: ');\n            }\n        },\n        _writeInt32: function (value, encode) {\n            const data = [];\n            let valueHigh = (value >> 16) & 0xffff;\n            let valueLow = value & 0xffff;\n            switch (encode) {\n                case DATA_ENCODE_ABCD:\n                    data.push(valueHigh, valueLow);\n                    break;\n                case DATA_ENCODE_BADC:\n                    data.push(\n                        ((valueHigh << 8) & 0xff00) | ((valueHigh >> 8) & 0x00ff),\n                        ((valueLow << 8) & 0xff00) | ((valueLow >> 8) & 0x00ff)\n                    );\n                    break;\n                case DATA_ENCODE_CDAB:\n                    data.push(valueLow, valueHigh);\n                    break;\n                case DATA_ENCODE_DCBA:\n                    data.push(\n                        ((valueLow << 8) & 0xff00) | ((valueLow >> 8) & 0x00ff),\n                        ((valueHigh << 8) & 0xff00) | ((valueHigh >> 8) & 0x00ff)\n                    );\n                    break;\n                default:\n                    throw new Error('不支持的数据字节序');\n            }\n            return data;\n        },\n\n        _writeFloat32: function (value, encode) {\n            const float32 = new Float32Array([value]);\n            const int32 = new Int32Array(float32.buffer);\n            return this._writeInt32(int32[0], encode);\n        },\n\n        _writeInt64: function (value, encode) {\n            const valueHigh = (value >> 32n) & 0xffffffffn;\n            const valueLow = value & 0xffffffffn;\n            return encode == DATA_ENCODE_CDAB || encode == DATA_ENCODE_DCBA\n                ? [...this._writeInt32(Number(valueLow), encode), ...this._writeInt32(Number(valueHigh), encode)]\n                : [...this._writeInt32(Number(valueHigh), encode), ...this._writeInt32(Number(valueLow), encode)];\n        },\n\n        _writeFloat64: function (value, encode) {\n            const float64 = new Float64Array([value]);\n            const int64 = new BigInt64Array(float64.buffer);\n            return this._writeInt64(int64[0], encode);\n        },\n        getWriteValue: function (slaveId, address, value) {\n            node.log('当前fc:'+this.fc);\n            const reqData = this.builderWriteRequestData(value);\n            if (COIL_STATUS_AREA == this.fc) {\n                return { parseVal: this._makeFC5(slaveId, address, reqData) , fc: 5};\n            } else if (HOLDING_REGISTER_AREA == this.fc) {\n                return [DATA_TYPE_UINT16, DATA_TYPE_INT16, DATA_TYPE_HEX].includes(this.dataType)\n                    ? { parseVal: reqData, fc: 6 }\n                    : { parseVal: reqData, fc: 16 };\n            } else {\n                throw new Error('不支持写入的分区dddd');\n            }\n        },\n    });\n}\n\nif (flow.get('configs') == undefined) {\n    const configs = flow.get('configs', 'file');\n    if (configs == undefined) {\n        node.log('获取配置为空');\n        return null;\n    } else {\n        flow.set('configs', configs);\n    }\n    node.log('重新加载配置');\n}\n\ntry{\n    const configs = flow.get('configs');\n    if(configs == undefined){\n        return null;\n    }\n    const requestMap = new Map();\n    const parser = global.get('valueParser');\n    for (const [key, value] of Object.entries(msg.payload)) {\n        for (const [configKey, item] of Object.entries(configs)) {\n            if (item[key] != undefined && item[key]['ioType'] != '只读') {\n                const serialNumber = configKey.split('-')[2];\n                let req;\n                if(item[key]['fc'] != undefined){\n                    parser.fc = item[key].fc;\n                    parser.dataType = item[key].dataType;\n                    parser.dataEncode = item[key].dataEncode;\n                    parser.dataIden = key;\n                    const { parseVal, fc} = parser.getWriteValue(item[key].unitid, item[key].address, value);\n                    req = {\n                        ...item[key],\n                        value: parseVal\n                    };\n                    req.fc = fc;\n                    node.log('req:'+JSON.stringify(req));\n                } else {\n                    req = {\n                        payload: value,\n                        variable: key\n                    };\n                }\n                if (requestMap.has(serialNumber)) {\n                    requestMap.get(serialNumber).push(req)\n                } else {\n                    requestMap.set(serialNumber, [req]);\n                }\n                break;\n            }\n        }\n    }\n    const data = [];\n    node.log(\"requestMap: \" + JSON.stringify(requestMap));\n    for(const [key, value] of requestMap.entries()){\n        data.push({\n            serialNumber: key,\n            requests: value,\n        })\n    }\n    msg.payload = data;\n    return msg;\n}catch(error){\n    node.error(error.message, msg);\n    return null;\n}";
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

        getInjectNode(writeId, cycleTime, props) {
          const injectNode = {
            id: `${writeId}-inject`,
            type: "inject",
            name: "",
            props: props,
            repeat: (cycleTime / 1000).toFixed(2),
            crontab: "",
            once: false,
            onceDelay: 0.1,
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
          const funcScript = "const requests = msg.payload.requests.map(req => {\n    node.log(\"value:\" + new Uint8Array(req.value))\n    return {\n        topic: `${req.unitid}-${req.address}`,\n        payload: {\n            address: req.address,\n            value: req.value,\n            unitid: req.unitid,\n            fc: req.fc,\n            quantity: req.value.length\n        }\n    };\n});\nnode.log(\"1requests:\" + JSON.stringify(requests));\nreturn [requests];";
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

      // 获取用户目录的正确方式
      const getUserDir = () => {
        // 方法1: 通过RED.settings直接获取
        if (RED.settings.userDir) {
          return RED.settings.userDir;
        }
        // 方法2: 环境变量回退
        return process.env.NODE_RED_HOME || path.join(require('os').homedir(), '.node-red');
      };

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
              node.context().flow.set('configs', data.channelConfigs, 'file');
              node.context().flow.set('reportAtBreakPoint', data.nodeConfigs.reportAtBreakPoint);
              // 立刻从文件读回并打印
              const saved = node.context().flow.get('configs', 'file');
              node.log('\n验证文件存储后的configs: ' + JSON.stringify(saved));
              /* 触发配置缓存更新 */
              node.context().flow.set('configs', undefined);
              node.log("通道配置更新成功");
              mqttClient.publish(topic + '_reply', JSON.stringify({
                id: payload.id,
                method: payload.method + "_reply",
                version: "1.0",
                code: 20000,
                message: "配置更新成功"
              }));
            } else {
              mqttClient.publish(topic + '_reply', JSON.stringify({
                id: payload.id,
                method: payload.method + "_reply",
                version: "1.0",
                code: 40000,
                message: "配置更新失败"
              }));
            }
          } catch (err) {
            node.error("处理消息时出错:" + err.message);
          }
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