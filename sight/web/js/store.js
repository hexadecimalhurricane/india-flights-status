const KEY = "sight.settings.v1";

const defaults = {
  proxyUrl: "",
  clientToken: "",
  voiceURI: "",
  rate: 1.05,
  earcons: true,
  ambient: true,
};

export const settings = { ...defaults, ...load() };

function load() {
  try { return JSON.parse(localStorage.getItem(KEY) || "{}"); } catch { return {}; }
}

export function saveSettings() {
  localStorage.setItem(KEY, JSON.stringify(settings));
}
