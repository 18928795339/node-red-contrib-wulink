const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const otaProtocol = require('./wulink-ota-protocol');

/**
 * node-red 需通过 service 或者 systemctl 启动，只有这样才能通过 exec 执行重启命令
 * OTA 协议见 https://dev.kunlun.cloud/docs/interfaceDocument/ota.html
 */
module.exports = function (RED) {
    function OtaUpgradeNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        node.configNode = RED.nodes.getNode(config.config);
        const mqttClient = node.configNode?.mqttClient;
        const userDir = RED.settings.userDir || process.cwd();
        const dataDir = path.join(userDir, 'data');
        const OTA_STATE_FILE = path.join(dataDir, 'wulink-ota-state.json');

        const productKey = node.configNode?.productKey;
        const deviceName = node.configNode?.deviceName;
        const currentVersion = config.currentVersion || '0.1.6';
        const restartStrategy = config.restartStrategy || 'custom-command';
        const restartCommand = config.restartCommand || 'service node-red restart';
        const restartDelayMs = Math.max(0, Number(config.restartDelayMs ?? 3000) || 3000);
        const packageBaseUrl = config.packageBaseUrl || '';
        const installCwd = config.installCwd || '/work/node-red';

        const upgradeTopic = productKey && deviceName
            ? otaProtocol.upgradeTopic(productKey, deviceName)
            : null;
        const progressTopicName = productKey && deviceName
            ? otaProtocol.progressTopic(productKey, deviceName)
            : null;

        let pendingTask = null;
        let upgradeMessageId = null;

        const writeJsonFileSync = (filePath, value) => {
            try {
                fs.mkdirSync(path.dirname(filePath), { recursive: true });
                fs.writeFileSync(filePath, JSON.stringify(value, null, 2), 'utf8');
            } catch (error) {
                node.warn(`写入文件失败(${filePath}): ${error.message}`);
            }
        };

        const updateOtaState = (patch) => {
            let previousState = {};
            try {
                if (fs.existsSync(OTA_STATE_FILE)) {
                    previousState = JSON.parse(fs.readFileSync(OTA_STATE_FILE, 'utf8'));
                }
            } catch (_) {
                previousState = {};
            }
            const nextState = {
                ...previousState,
                ...patch,
                updatedAt: new Date().toISOString()
            };
            writeJsonFileSync(OTA_STATE_FILE, nextState);
            return nextState;
        };

        const publishJson = (topic, message) => {
            if (!mqttClient?.connected || !topic) {
                return;
            }
            mqttClient.publish(topic, JSON.stringify(message), { qos: 1 });
        };

        const publishProgress = (percent, step, desc) => {
            if (!progressTopicName) {
                return;
            }
            publishJson(
                progressTopicName,
                otaProtocol.buildProgressMessage({ percent, step, desc }, upgradeMessageId)
            );
        };

        const reportFailure = (step, desc, statusPatch = {}) => {
            publishProgress(1, step, desc);
            updateOtaState({
                status: statusPatch.status || 'install_failed',
                error: desc,
                ...statusPatch
            });
            node.status({ fill: 'red', shape: 'ring', text: desc.slice(0, 20) });
            node.error(desc);
        };

        const buildInstallCommand = (packageUrl) => `npm install "${packageUrl}"`;

        const resolveInstallUrl = (url) => {
            try {
                return otaProtocol.resolvePackageUrl(url, packageBaseUrl);
            } catch (error) {
                reportFailure(
                    otaProtocol.PROGRESS_STEP.DOWNLOAD_FAILED,
                    error.message,
                    { status: 'install_failed' }
                );
                return null;
            }
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
                        updateOtaState({
                            status: 'restart_failed',
                            restartError: error.message,
                            restartFinishedAt: new Date().toISOString()
                        });
                        if (pendingTask) {
                            reportFailure(
                                otaProtocol.PROGRESS_STEP.FAILED,
                                `OTA 重启失败: ${error.message}`,
                                { status: 'restart_failed' }
                            );
                        }
                        node.status({ fill: 'red', shape: 'ring', text: 'OTA 重启失败' });
                    }
                });
            }, restartDelayMs);
        };

        const executeInstallCommand = (upgradeInfo) => {
            const { url, version, raw: payload } = upgradeInfo;
            const fullUrl = resolveInstallUrl(url);
            if (!fullUrl) {
                return;
            }
            const installCommand = buildInstallCommand(fullUrl);

            updateOtaState({
                status: 'installing',
                productKey,
                deviceName,
                currentVersion,
                targetVersion: version,
                packageUrl: url,
                packageFullUrl: fullUrl,
                upgradeInfo,
                installCommand,
                restartStrategy,
                restartCommand,
                restartDelayMs,
                request: payload,
                upgradeMessageId,
                installRequestedAt: new Date().toISOString()
            });

            node.log(`OTA 开始 npm install: ${fullUrl}`);
            publishProgress(30, otaProtocol.PROGRESS_STEP.UPGRADING, '开始下载安装包');
            node.status({ fill: 'blue', shape: 'dot', text: '安装中' });

            exec(installCommand, { cwd: installCwd, timeout: 600000 }, (error, stdout, stderr) => {
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
                    reportFailure(
                        otaProtocol.PROGRESS_STEP.DOWNLOAD_FAILED,
                        `OTA 安装失败: ${error.message}`
                    );
                    return;
                }

                publishProgress(70, otaProtocol.PROGRESS_STEP.UPGRADING, '安装包下载安装完成');
                updateOtaState({
                    status: 'restore_needed',
                    installFinishedAt: new Date().toISOString(),
                    installResult: { stdout, stderr },
                    packageUrl: url,
                    packageFullUrl: fullUrl,
                    targetVersion: version,
                    restartDelayMs
                });
                publishProgress(80, otaProtocol.PROGRESS_STEP.UPGRADING, 'OTA 安装完成，准备重启 Node-RED');
                scheduleRestart();
            });
        };

        const handleOtaCommand = (payload) => {
            const upgradeInfo = otaProtocol.parseUpgradePayload(payload);
            upgradeMessageId = upgradeInfo.id ? String(upgradeInfo.id) : otaProtocol.generateMessageId();

            pendingTask = {
                id: upgradeMessageId,
                packageUrl: upgradeInfo.url,
                targetVersion: upgradeInfo.version,
                payload,
                receivedAt: new Date().toISOString()
            };

            publishProgress(10, otaProtocol.PROGRESS_STEP.UPGRADING, 'OTA 命令已接收，开始下载安装包');
            executeInstallCommand(upgradeInfo);
        };

        const handleMqttMessage = (topic, message) => {
            if (topic !== upgradeTopic) {
                return;
            }

            try {
                node.log(`收到 OTA 升级消息: ${message}`);
                const payload = JSON.parse(message.toString());
                handleOtaCommand(payload);
            } catch (error) {
                node.status({ fill: 'red', shape: 'ring', text: 'OTA 消息处理失败' });
                try {
                    const payload = JSON.parse(message.toString());
                    upgradeMessageId = payload?.id ? String(payload.id) : otaProtocol.generateMessageId();
                    reportFailure(
                        otaProtocol.PROGRESS_STEP.FAILED,
                        `OTA 消息处理失败: ${error.message}`
                    );
                } catch (_) {
                    node.warn('OTA 消息解析失败，无法上报进度');
                }
            }
        };

        const subscribeUpgradeTopic = () => {
            node.status({ fill: 'green', shape: 'dot', text: '已连接' });

            if (!upgradeTopic) {
                node.error('缺少 productKey/deviceName，无法订阅 OTA 升级 Topic');
                return;
            }

            mqttClient.subscribe(upgradeTopic, { qos: 1 }, (err) => {
                if (err) {
                    node.error(`订阅 OTA 升级 Topic 失败: ${err.message}`);
                    node.status({ fill: 'red', shape: 'ring', text: 'OTA订阅失败' });
                    return;
                }
                node.log(`已订阅 OTA 升级 Topic: ${upgradeTopic}`);
            });
        };

        if (!node.configNode || !mqttClient) {
            node.status({ fill: 'red', shape: 'ring', text: 'MQTT未连接' });
            node.error('未获取到 MQTT 客户端，请检查 wulink-config 节点');
        } else {
            if (mqttClient.connected) {
                subscribeUpgradeTopic();
            }
            mqttClient.on('connect', subscribeUpgradeTopic);
            mqttClient.on('message', handleMqttMessage);
        }

        if (node.configNode) {
            node.configNode.on('status', (status) => {
                node.status(status);
            });
        }

        node.on('close', () => {
            if (mqttClient) {
                mqttClient.removeListener('connect', subscribeUpgradeTopic);
                mqttClient.removeListener('message', handleMqttMessage);
            }
            node.status({});
        });
    }

    RED.nodes.registerType('ota-upgrade', OtaUpgradeNode);
};
