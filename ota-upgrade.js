const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');

module.exports = function (RED) {
    function OtaUpgradeNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        node.configNode = RED.nodes.getNode(config.config);
        const mqttClient = node.configNode?.mqttClient;
        const PERSIST_STORE = 'file';
        const OTA_STATE_KEY = 'wulink:otaState';
        const userDir = RED.settings.userDir || process.cwd();
        const dataDir = path.join(userDir, 'data');
        const OTA_STATE_FILE = path.join(dataDir, 'wulink-ota-state.json');
        const { productKey, deviceName } = node.configNode;
        const otaTopic = config.otaTopic || `/sys/${productKey}/${deviceName}/ota/update/push`;
        const packageUrlField = config.packageUrlField || 'data.packageUrl';
        const restartStrategy = config.restartStrategy || 'custom-command';
        const restartCommand = config.restartCommand || 'service node-red restart';
        const restartDelayMs = Math.max(0, Number(config.restartDelayMs ?? 3000) || 3000);

        let pendingTask = null;
        const writeJsonFileSync = (filePath, value) => {
            try {
                fs.mkdirSync(path.dirname(filePath), { recursive: true });
                fs.writeFileSync(filePath, JSON.stringify(value, null, 2), 'utf8');
            } catch (error) {
                node.warn(`写入文件失败(${filePath}): ${error.message}`);
            }
        };

        const getByPath = (source, fieldPath) => {
            if (!fieldPath) {
                return undefined;
            }
            return fieldPath.split('.').reduce((current, key) => {
                if (current == null) {
                    return undefined;
                }
                return current[key];
            }, source);
        };

        const publishReply = (payload, code, message, extra = {}) => {
            mqttClient.publish(`${otaTopic}_reply`, JSON.stringify({
                id: payload?.id,
                method: payload?.method ? `${payload.method}_reply` : undefined,
                version: payload?.version || '1.0',
                code,
                message,
                ...extra
            }), { qos: 1 });
        };

        const updateOtaState = (patch) => {
            const nextState = {
                ...patch,
                updatedAt: new Date().toISOString()
            };

            writeJsonFileSync(OTA_STATE_FILE, nextState);
            return nextState;
        };

        const buildInstallCommand = (packageUrl) => `npm install "${packageUrl}"`;

        const executeInstallCommand = (payload, packageUrl) => {
            const installCommand = buildInstallCommand(packageUrl);

            updateOtaState({
                status: 'installing',
                otaTopic,
                packageUrl,
                packageUrlField,
                installCommand,
                restartStrategy,
                restartCommand,
                restartDelayMs,
                request: payload,
                installRequestedAt: new Date().toISOString()
            });

            node.status({ fill: 'blue', shape: 'dot', text: '安装中' });
            exec(installCommand, { cwd: '/work/node-red', timeout: 60000 }, (error, stdout, stderr) => {
                if (stdout) {
                    node.log(`安装命令输出: ${stdout}`);
                }
                if (stderr) {
                    node.warn(`安装命令告警: ${stderr}`);
                }

                if (error) {
                    updateOtaState({
                        status: 'install_failed',
                        installError: error.message,
                        installResult: { stdout, stderr }
                    });
                    node.status({ fill: 'red', shape: 'ring', text: 'OTA 安装失败' });
                    publishReply(payload, 40000, `OTA 安装失败: ${error.message}`);
                    node.error(`OTA 安装失败: ${error.message}`);
                    return;
                }

                updateOtaState({
                    status: 'restore_needed',
                    installFinishedAt: new Date().toISOString(),
                    installResult: { stdout, stderr },
                    packageUrl,
                    otaTopic,
                    restartDelayMs
                });
                publishReply(payload, 20000, 'OTA 安装完成，准备重启 Node-RED');
                scheduleRestart();
            });
        };

        const scheduleRestart = () => {
            node.warn(`OTA 安装完成，将在 ${restartDelayMs}ms 后执行重启`);
            node.status({ fill: 'yellow', shape: 'dot', text: `安装完成，${Math.ceil(restartDelayMs / 1000)}秒后重启` });

            setTimeout(() => {
                if (restartStrategy === 'process-exit') {
                    node.warn('即将执行 process.exit(0) 以等待外部守护进程拉起 Node-RED');
                    process.exit(0);
                    return;
                }

                node.warn(`开始执行重启命令: ${restartCommand}`);
                exec(restartCommand, (error, stdout, stderr) => {
                    if (stdout) {
                        node.log(`重启命令输出: ${stdout}`);
                    }
                    if (stderr) {
                        node.warn(`重启命令告警: ${stderr}`);
                    }
                    if (error) {
                        node.warn(`重启命令执行失败: ${error}`);
                        updateOtaState({
                            status: 'restart_failed',
                            restartError: error.message,
                            restartFinishedAt: new Date().toISOString()
                        });
                        if (pendingTask?.payload) {
                            publishReply(pendingTask.payload, 40000, `OTA 重启失败: ${error.message}`);
                        }
                        node.status({ fill: 'red', shape: 'ring', text: 'OTA 重启失败' });
                        node.error(`OTA 重启失败: ${error.message}`);
                    }
                });
            }, restartDelayMs);
        };

        const handleOtaCommand = (payload) => {
            const packageUrl = getByPath(payload, packageUrlField);
            if (!packageUrl) {
                throw new Error(`OTA 消息中未找到升级包链接字段: ${packageUrlField}`);
            }

            pendingTask = {
                id: payload?.id,
                method: payload?.method,
                packageUrl,
                payload,
                receivedAt: new Date().toISOString()
            };
            publishReply(payload, 20000, 'OTA 命令已接收，开始下载安装包');
            executeInstallCommand(payload, packageUrl);
        };

        const handleMqttMessage = (topic, message) => {
            if (topic !== otaTopic) {
                return;
            }

            try {
                node.log(`收到 OTA 消息: ${message}`);
                const payload = JSON.parse(message.toString());
                handleOtaCommand(payload);
            } catch (error) {
                node.status({ fill: 'red', shape: 'ring', text: 'OTA 消息处理失败' });
                try {
                    const payload = JSON.parse(message.toString());
                    publishReply(payload, 40000, `OTA 消息处理失败: ${error.message}`);
                } catch (_) {
                    node.warn('OTA 消息解析失败，无法回传带请求ID的错误响应');
                }
                node.error(`OTA 消息处理失败: ${error.message}`);
            }
        };

        const subscribeTopic = () => {
            node.status({ fill: 'green', shape: 'dot', text: '已连接' });
            mqttClient.subscribe(otaTopic, { qos: 1 }, (err) => {
                if (err) {
                    node.error(`订阅 OTA Topic 失败: ${err.message}`);
                    node.status({ fill: 'red', shape: 'ring', text: 'OTA订阅失败' });
                    return;
                }
                node.log(`已订阅 OTA Topic: ${otaTopic}`);
            });
        };

        if (!node.configNode || !mqttClient) {
            node.status({ fill: 'red', shape: 'ring', text: 'MQTT未连接' });
            node.error('未获取到 MQTT 客户端，请检查 wulink-config 节点');
        } else {
            if (mqttClient.connected) {
                subscribeTopic();
            }
            mqttClient.on('connect', subscribeTopic);
            mqttClient.on('message', handleMqttMessage);
        }
        if (node.configNode) {
            node.configNode.on('status', (status) => {
                node.status(status);
            });
        }

        node.on('close', () => {
            if (node.configNode?.mqttClient) {
                node.configNode.mqttClient.removeListener('connect', subscribeTopic);
                node.configNode.mqttClient.removeListener('message', handleMqttMessage);
            }
            node.status({});
        });
    }

    RED.nodes.registerType('ota-upgrade', OtaUpgradeNode);
};