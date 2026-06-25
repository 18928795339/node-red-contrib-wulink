const crypto = require('crypto');

const PROGRESS_STEP = {
  UPGRADING: '2',
  FAILED: '5',
  DOWNLOAD_FAILED: '6',
  VERIFY_FAILED: '7',
  BURN_FAILED: '8'
};

function informTopic(productKey, deviceName) {
  return `/ota/device/inform/${productKey}/${deviceName}`;
}

function upgradeTopic(productKey, deviceName) {
  return `/ota/device/upgrade/${productKey}/${deviceName}`;
}

function progressTopic(productKey, deviceName) {
  return `/ota/device/progress/${productKey}/${deviceName}`;
}

function firmwareGetTopic(productKey, deviceName) {
  return `/ota/firmware/get/${productKey}/${deviceName}`;
}

function generateMessageId() {
  return String(Date.now());
}

function buildInformMessage(version, id) {
  return {
    id: id || generateMessageId(),
    params: { version }
  };
}

function buildProgressMessage({ percent, step, desc }, id) {
  const params = {
    percent: String(percent),
    step: String(step),
    desc: desc || ''
  };
  return {
    id: id || generateMessageId(),
    params
  };
}

function buildFirmwareGetMessage(id) {
  return {
    id: id || generateMessageId(),
    version: '1.0',
    method: 'thing.ota.firmware.get'
  };
}

function parseUpgradePayload(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('OTA 升级消息格式无效');
  }

  const code = payload.code;
  if (code !== undefined && code !== 20000 && code !== '20000') {
    throw new Error(`OTA 升级消息状态码异常: ${code}`);
  }

  const data = payload.data;
  if (!data || typeof data !== 'object') {
    throw new Error('OTA 升级消息缺少 data 字段');
  }

  const url = data.url;
  if (!url) {
    throw new Error('OTA 升级消息缺少 data.url');
  }

  return {
    id: payload.id,
    url,
    version: data.version,
    size: data.size,
    sign: data.sign,
    signMethod: data.signMethod,
    md5: data.md5,
    raw: payload
  };
}

function verifyPackageSignature(buffer, upgradeInfo) {
  const { sign, signMethod, md5 } = upgradeInfo;
  if (!sign && !md5) {
    return true;
  }

  const method = (signMethod || (md5 ? 'MD5' : '')).toUpperCase();
  let digest;

  if (method === 'MD5' || md5) {
    digest = crypto.createHash('md5').update(buffer).digest('hex');
    const expected = (md5 || sign || '').toLowerCase();
    return digest.toLowerCase() === expected;
  }

  if (method === 'SHA256') {
    digest = crypto.createHash('sha256').update(buffer).digest('hex');
    return digest.toLowerCase() === String(sign).toLowerCase();
  }

  return true;
}

function resolvePackageUrl(url, packageBaseUrl) {
  if (!url) {
    throw new Error('缺少升级包 url');
  }
  if (/^https?:\/\//i.test(url)) {
    return url;
  }
  const base = packageBaseUrl || 'https://wulink.tech';
  return `${base}${url}`;
}

module.exports = {
  PROGRESS_STEP,
  informTopic,
  upgradeTopic,
  progressTopic,
  firmwareGetTopic,
  generateMessageId,
  buildInformMessage,
  buildProgressMessage,
  buildFirmwareGetMessage,
  parseUpgradePayload,
  verifyPackageSignature,
  resolvePackageUrl
};
