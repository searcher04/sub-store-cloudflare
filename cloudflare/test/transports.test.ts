import { describe, expect, it } from "vitest";
import { buildSubscription, convertSubscriptionContent, validateSubscriptionContent } from "../src/lib/subscription";
import { getProxyTransport, transportCompatibilityError } from "../src/lib/transports";

const uuid = "00000000-0000-4000-8000-000000000002";
const base = { type: "vless", name: "Unrelated name", server: "example.com", port: 443, uuid, tls: true, servername: "tls.example.com" };

function build(content: string, target: "uri" | "v2ray" | "json" | "mihomo" | "sing-box", filters = [] as Array<{ type: string; field: string; pattern: string }>) {
  return buildSubscription({
    source: { id: "transport", name: "Transport", type: "local", url: "", content, filters },
    sources: [], target, requestUrl: new URL("https://example.com/download/source/transport"),
  });
}

describe("lossless transport conversion", () => {
  it("preserves legacy H2 path and hosts without guessing XHTTP from names", async () => {
    const node = { ...base, name: "Xhttp-test", network: "h2", "h2-opts": { path: "/edge?key=1", host: ["cdn.example.com", "cdn2.example.com"] } };
    const input = JSON.stringify({ proxies: [node] });
    const uri = await build(input, "uri");
    const params = new URL(uri).searchParams;
    expect(params.get("type")).toBe("http");
    expect(params.get("path")).toBe("/edge?key=1");
    expect(params.get("host")).toBe("cdn.example.com,cdn2.example.com");
    expect(validateSubscriptionContent(uri)[0]).toMatchObject({ network: "h2", "h2-opts": node["h2-opts"] });
    const singbox = JSON.parse(await build(input, "sing-box"));
    expect(singbox.outbounds.find((entry: { type: string }) => entry.type === "vless").transport).toEqual({ type: "http", path: "/edge?key=1", host: node["h2-opts"].host });
    await expect(build(input, "v2ray")).rejects.toThrow("current Xray/v2rayN removed legacy H2");
  });

  it.each([
    ["ws", "path=%2Fedge&host=cdn.example.com"],
    ["grpc", "serviceName=edge%2Fservice"],
    ["httpupgrade", "path=%2Fedge&host=cdn.example.com"],
    ["xhttp", "path=%2Fedge&host=cdn.example.com&mode=stream-up&extra=%7B%22noGRPCHeader%22%3Atrue%2C%22xPaddingBytes%22%3A%220%22%7D"],
  ])("preserves VLESS %s from URI through JSON, Mihomo and URI", async (network, query) => {
    const input = `vless://${uuid}@example.com:443?security=tls&sni=tls.example.com&type=${network}&${query}#Node`;
    const json = await build(input, "json");
    const uri = await build(json, "uri");
    expect(validateSubscriptionContent(uri)[0]).toEqual(validateSubscriptionContent(input)[0]);
    const encoded = await build(json, "v2ray");
    expect(new TextDecoder().decode(Uint8Array.from(atob(encoded), (char) => char.charCodeAt(0)))).toBe(uri);
    const yaml = await build(json, "mihomo");
    expect(getProxyTransport(validateSubscriptionContent(yaml)[0])).toBe(network);
    if (network !== "httpupgrade") {
      expect(validateSubscriptionContent(yaml)[0]).toMatchObject(JSON.parse(json).proxies[0]);
    } else expect(validateSubscriptionContent(yaml)[0]).toMatchObject({ network: "ws", "ws-opts": { path: "/edge", "v2ray-http-upgrade": true } });
    if (network === "xhttp") await expect(build(json, "sing-box")).rejects.toThrow("cannot represent xhttp");
    else {
      const singbox = JSON.parse(await build(json, "sing-box"));
      expect(singbox.outbounds.find((entry: { type: string }) => entry.type === "vless").transport.type).toBe(network);
    }
  });

  it("reports unsupported transports during conversion and rejects corrupt subscription output", async () => {
    const unsupported = { ...base, name: "Unsupported", network: "unknown-transport" };
    const valid = { ...base, name: "Valid", network: "tcp" };
    const content = JSON.stringify({ proxies: [unsupported, valid] });
    await expect(convertSubscriptionContent({ content, target: "v2ray" })).rejects.toThrow("Unsupported: v2ray cannot represent unknown-transport transport");
    await expect(build(content, "v2ray")).rejects.toThrow("Unsupported: v2ray cannot represent unknown-transport");
  });

  it("filters protocol and transport independently, including omitted and RAW TCP", async () => {
    const nodes = [
      { ...base, name: "Default TCP" },
      { ...base, name: "RAW", network: "raw" },
      { ...base, name: "Actual XHTTP", network: "xhttp", "xhttp-opts": { path: "/edge" } },
      { ...base, name: "Old H2", network: "h2", "h2-opts": { path: "/edge" } },
    ];
    const content = JSON.stringify({ proxies: nodes });
    const typeFiltered = JSON.parse(await build(content, "json", [{ type: "include", field: "type", pattern: "^vless$" }]));
    expect(typeFiltered.proxies).toHaveLength(4);
    const tcpOnly = JSON.parse(await build(content, "json", [{ type: "include", field: "network", pattern: "^tcp$" }]));
    expect(tcpOnly.proxies.map((node: { name: string }) => node.name)).toEqual(["Default TCP", "RAW"]);
    expect(getProxyTransport({ ...base, network: "ws", "ws-opts": { "v2ray-http-upgrade": true } })).toBe("httpupgrade");
  });

  it("preserves Trojan WebSocket and VMess gRPC/H2 parameters", async () => {
    const trojan = "trojan://password@example.com:443?type=ws&path=%2Fedge&host=cdn.example.com#Trojan";
    const output = await build(trojan, "uri");
    expect(validateSubscriptionContent(output)[0]).toMatchObject({ network: "ws", "ws-opts": { path: "/edge", headers: { Host: "cdn.example.com" } } });
    const singbox = JSON.parse(await build(trojan, "sing-box"));
    expect(singbox.outbounds.find((node: { type: string }) => node.type === "trojan").transport.path).toBe("/edge");
    for (const network of ["grpc", "h2"]) {
      const options = network === "grpc" ? { "grpc-service-name": "edge/service" } : { path: "/edge", host: ["cdn.example.com"] };
      const proxy = { ...base, type: "vmess", network, [network + "-opts"]: options };
      const uri = await build(JSON.stringify({ proxies: [proxy] }), "uri");
      expect(validateSubscriptionContent(uri)[0]).toMatchObject({ network, [network + "-opts"]: options });
    }
  });

  it("does not silently lose extra headers or unsupported gRPC modes", () => {
    expect(transportCompatibilityError({ ...base, network: "ws", "ws-opts": { headers: { "X-Custom": "value" } } }, "uri")).toContain("cannot preserve custom headers");
    expect(transportCompatibilityError({ ...base, network: "grpc", "grpc-opts": { mode: "multi" } }, "sing-box")).toContain("cannot preserve gRPC mode");
  });
});
