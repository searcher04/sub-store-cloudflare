import type { ProxyNode, SubscriptionTarget } from "../types";

type Options = Record<string, unknown>;
export class TransportConversionError extends Error {}
const TRANSPORT_PROTOCOLS = new Set(["vless", "vmess", "trojan"]);
const XHTTP_EXTRA_FIELDS: Record<string, string> = {
  "no-grpc-header": "noGRPCHeader",
  "x-padding-bytes": "xPaddingBytes",
  "sc-max-each-post-bytes": "scMaxEachPostBytes",
  "sc-min-posts-interval-ms": "scMinPostsIntervalMs",
  "reuse-settings": "xmux",
  "download-settings": "downloadSettings",
};

export function getProxyTransport(proxy: ProxyNode): string {
  const network = String(proxy.network || "tcp").toLowerCase();
  if (network === "ws" && object(proxy["ws-opts"])["v2ray-http-upgrade"] === true) return "httpupgrade";
  if (network === "raw") return "tcp";
  if (network === "splithttp") return "xhttp";
  return network;
}

function object(input: unknown): Options {
  return input && typeof input === "object" && !Array.isArray(input) ? input as Options : {};
}

function compact(input: Options): Options {
  return Object.fromEntries(Object.entries(input).filter(([, value]) => value !== undefined));
}

export function transportOptions(proxy: ProxyNode): Options {
  return object(proxy[`${getProxyTransport(proxy)}-opts`]);
}

export function parseUriTransport(params: URLSearchParams): Options {
  const type = params.get("type") || "tcp";
  const network = type === "http" || type === "h2" ? "h2"
    : type === "splithttp" ? "xhttp"
      : type === "raw" ? "tcp"
        : type === "tcp" && params.get("headerType") === "http" ? "http" : type;
  const path = params.get("path") || "/";
  const host = params.get("host") || undefined;
  if (network === "ws" || network === "httpupgrade") {
    return { network, "ws-opts": compact({ path, headers: host ? { Host: host } : undefined,
      "v2ray-http-upgrade": network === "httpupgrade" ? true : undefined }) };
  }
  if (network === "h2") return { network, "h2-opts": compact({ path, host: host ? host.split(",") : undefined }) };
  if (network === "http") return { network, "http-opts": compact({ path: [path], headers: host ? { Host: host.split(",") } : undefined }) };
  if (network === "grpc") {
    return { network, "grpc-opts": compact({
      "grpc-service-name": params.get("serviceName") || "",
      mode: params.get("mode") || undefined,
      authority: params.get("authority") || undefined,
    }) };
  }
  if (network === "xhttp") {
    const opts: Options = compact({ path, host, mode: params.get("mode") || "auto" });
    if (params.has("extra")) {
      const extra = object(JSON.parse(params.get("extra") || "{}"));
      const reverse = Object.fromEntries(Object.entries(XHTTP_EXTRA_FIELDS).map(([key, value]) => [value, key]));
      for (const [key, value] of Object.entries(extra)) opts[reverse[key] || key] = value;
    }
    return { network, "xhttp-opts": opts };
  }
  return { network };
}

function websocketOptions(proxy: ProxyNode): Options {
  return object(proxy["ws-opts"]);
}

function hostValue(input: unknown): string | undefined {
  if (Array.isArray(input)) return input.map(String).join(",") || undefined;
  return input === undefined ? undefined : String(input);
}

export function appendUriTransport(params: URLSearchParams, proxy: ProxyNode): void {
  const network = getProxyTransport(proxy);
  const opts = transportOptions(proxy);
  params.set("type", network === "h2" ? "http" : network === "http" ? "tcp" : network);
  if (network === "tcp") return;
  if (network === "ws" || network === "httpupgrade") {
    const ws = websocketOptions(proxy);
    params.set("path", String(ws.path || "/"));
    const headers = object(ws.headers);
    const host = hostValue(headers.Host || headers.host);
    if (host) params.set("host", host);
    return;
  }
  if (network === "grpc") {
    params.set("serviceName", String(opts["grpc-service-name"] || ""));
    if (opts.mode) params.set("mode", String(opts.mode));
    if (opts.authority) params.set("authority", String(opts.authority));
    return;
  }
  if (network === "http") params.set("headerType", "http");
  params.set("path", Array.isArray(opts.path) ? opts.path.map(String).join(",") : String(opts.path || "/"));
  const headers = object(opts.headers);
  const host = hostValue(opts.host || headers.Host || headers.host);
  if (host) params.set("host", host);
  if (network !== "xhttp") return;
  params.set("mode", String(opts.mode || "auto"));
  const extra: Options = {};
  for (const [key, value] of Object.entries(opts)) {
    if (!["path", "host", "mode"].includes(key)) extra[XHTTP_EXTRA_FIELDS[key] || key] = value;
  }
  if (Object.keys(extra).length) params.set("extra", JSON.stringify(extra));
}

export function singBoxTransport(proxy: ProxyNode): Options | undefined {
  const network = getProxyTransport(proxy);
  if (network === "tcp") return undefined;
  const opts = transportOptions(proxy);
  if (network === "ws" || network === "httpupgrade") {
    const ws = websocketOptions(proxy);
    const headers = object(ws.headers);
    return compact({ type: network, path: ws.path || "/", headers: ws.headers,
      host: network === "httpupgrade" ? headers.Host || headers.host : undefined,
      max_early_data: ws["max-early-data"], early_data_header_name: ws["early-data-header-name"] });
  }
  if (network === "h2") return compact({ type: "http", path: opts.path || "/", host: opts.host });
  if (network === "http") return compact({ type: "http", path: Array.isArray(opts.path) ? opts.path[0] : opts.path || "/", headers: opts.headers, method: opts.method });
  if (network === "grpc") return { type: "grpc", service_name: opts["grpc-service-name"] || "" };
  throw new Error(`sing-box cannot represent ${network} transport`);
}

export function transportCompatibilityError(proxy: ProxyNode, target: SubscriptionTarget): string | undefined {
  if (!TRANSPORT_PROTOCOLS.has(proxy.type) || target === "json") return undefined;
  const network = getProxyTransport(proxy);
  if (network === "xhttp" && proxy.type !== "vless") return `${proxy.name}: XHTTP is only supported for VLESS`;
  const supported: string[] = target === "mihomo" ? ["tcp", "ws", "grpc", "h2", "http", "xhttp"]
    : target === "stash" ? ["tcp", "ws", "grpc", "h2", "http"]
      : target === "sing-box" ? ["tcp", "ws", "grpc", "h2", "http", "httpupgrade"]
        : target === "uri" || target === "shadowrocket" ? ["tcp", "ws", "grpc", "h2", "http", "httpupgrade", "xhttp"]
          : target === "v2ray" ? ["tcp", "ws", "grpc", "http", "httpupgrade", "xhttp"]
            : ["tcp", "ws"];
  if (!supported.includes(network)) {
    const detail = target === "v2ray" && network === "h2"
      ? "; current Xray/v2rayN removed legacy H2. Obtain an actual XHTTP source or use the transport filter"
      : "";
    return `${proxy.name}: ${target} cannot represent ${network} transport${detail}`;
  }
  const opts = transportOptions(proxy);
  if (network === "xhttp" && target === "mihomo") {
    const allowed = ["path", "host", "mode", "headers", ...Object.keys(XHTTP_EXTRA_FIELDS)];
    if (Object.keys(opts).some((key) => !allowed.includes(key)) || opts["reuse-settings"] || opts["download-settings"]) {
      return `${proxy.name}: mihomo cannot preserve these extended XHTTP settings`;
    }
  }
  if (target === "sing-box" && network === "grpc" && (opts.authority || (opts.mode && opts.mode !== "gun"))) {
    return `${proxy.name}: sing-box cannot preserve gRPC mode/authority`;
  }
  if (["mihomo", "stash"].includes(target) && network === "grpc" && (opts.authority || (opts.mode && opts.mode !== "gun"))) {
    return `${proxy.name}: ${target} cannot preserve gRPC mode/authority`;
  }
  if (["surge", "surge-mac", "surfboard", "loon", "qx", "egern"].includes(target) && network === "ws" && proxy.type === "trojan") {
    return `${proxy.name}: ${target} cannot preserve Trojan WebSocket settings`;
  }
  const uriTarget = ["uri", "v2ray", "shadowrocket"].includes(target);
  if ((uriTarget || target === "sing-box") && network === "http" && Array.isArray(opts.path) && opts.path.length > 1) {
    return `${proxy.name}: ${target} cannot preserve multiple HTTP paths`;
  }
  if (uriTarget && ["ws", "httpupgrade", "http"].includes(network)) {
    const sourceOpts = network === "http" ? opts : websocketOptions(proxy);
    if (Object.keys(object(sourceOpts.headers)).some((key) => key.toLowerCase() !== "host")
      || sourceOpts["max-early-data"] || sourceOpts["early-data-header-name"]) {
      return `${proxy.name}: ${target} cannot preserve custom headers/early data for ${network}`;
    }
  }
  return undefined;
}

export function assertTransportCompatibility(proxies: ProxyNode[], target: SubscriptionTarget): void {
  const errors = proxies.map((proxy) => transportCompatibilityError(proxy, target)).filter(Boolean);
  if (errors.length) throw new TransportConversionError(`Subscription conversion failed (${errors.length} node(s)): ${errors.slice(0, 3).join("; ")}`);
}
