const components = '("@status" "content-digest" "content-type" "@method";req "@target-uri";req)';
const label = "sig1";
const tag = "fh-rfc9421-response";
const encoder = new TextEncoder();

function base64(bytes) {
  let value = "";
  for (const byte of bytes) value += String.fromCharCode(byte);
  return btoa(value);
}

function base64url(bytes) {
  return base64(bytes).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

function decodeBase64(value) {
  const raw = atob(value);
  return Uint8Array.from(raw, (character) => character.charCodeAt(0));
}

function dictionaryMember(field, name) {
  const prefix = `${name}=`;
  if (!field.startsWith(prefix) || field.includes(",")) throw new Error(`Malformed ${name} dictionary member`);
  return field.slice(prefix.length);
}

async function verifyResponse(request, response, body, nonce, config) {
  const digest = response.headers.get("content-digest") ?? "";
  const actualHash = new Uint8Array(await crypto.subtle.digest("SHA-256", body));
  const expectedDigest = `sha-256=:${base64(actualHash)}:`;
  if (digest !== expectedDigest) throw new Error("Content-Digest verification failed");

  const input = dictionaryMember(response.headers.get("signature-input") ?? "", label);
  const pattern = new RegExp(
    `^${components.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")};created=(\\d+);expires=(\\d+);nonce=\"([A-Za-z0-9_-]+)\";keyid=\"([^\"\\r\\n]+)\";alg=\"ed25519\";tag=\"${tag}\"$`,
  );
  const match = input.match(pattern);
  if (!match) throw new Error("Unsupported Signature-Input profile");
  const [, createdValue, expiresValue, signedNonce, keyID] = match;
  const created = Number(createdValue);
  const expires = Number(expiresValue);
  const now = Math.floor(Date.now() / 1000);
  if (signedNonce !== nonce) throw new Error("Signature nonce mismatch");
  if (keyID !== config.keyID) throw new Error("Signature key ID mismatch");
  if (created > now + 30 || expires < now - 30 || expires <= created || expires - created > 120) {
    throw new Error("Signature lifetime is invalid");
  }

  const contentType = response.headers.get("content-type") ?? "";
  const base =
    `"@status": ${String(response.status).padStart(3, "0")}\n` +
    `"content-digest": ${digest}\n` +
    `"content-type": ${contentType}\n` +
    `"@method";req: ${request.method}\n` +
    `"@target-uri";req: ${request.url}\n` +
    `"@signature-params": ${input}`;
  const signatureMember = dictionaryMember(response.headers.get("signature") ?? "", label);
  if (!signatureMember.startsWith(":") || !signatureMember.endsWith(":")) throw new Error("Malformed Signature field");
  const signature = decodeBase64(signatureMember.slice(1, -1));
  const publicKey = await crypto.subtle.importKey("raw", decodeBase64URL(config.publicKey), "Ed25519", false, ["verify"]);
  const valid = await crypto.subtle.verify("Ed25519", publicKey, signature, encoder.encode(base));
  if (!valid) throw new Error("Ed25519 response signature verification failed");
}

function decodeBase64URL(value) {
  const padded = value.replaceAll("-", "+").replaceAll("_", "/") + "===".slice((value.length + 3) % 4);
  return decodeBase64(padded);
}

async function signedFetch(url, config) {
  const nonceBytes = crypto.getRandomValues(new Uint8Array(32));
  const nonce = base64url(nonceBytes);
  nonceBytes.fill(0);
  const accept = `${label}=${components};created;expires;nonce="${nonce}";keyid="${config.keyID}";alg="ed25519";tag="${tag}"`;
  const request = new Request(url, { headers: { "accept-signature": accept }, cache: "no-store" });
  const response = await fetch(request);
  const body = await response.arrayBuffer();
  await verifyResponse(request, response, body, nonce, config);
  return { response, body: new Uint8Array(body) };
}

const output = document.querySelector("#output");
document.querySelector("#request").addEventListener("click", async () => {
  try {
    const configResponse = await fetch("/demo-config.json", { cache: "no-store" });
    if (!configResponse.ok) throw new Error(`Configuration failed: HTTP ${configResponse.status}`);
    const config = await configResponse.json();
    const { response, body } = await signedFetch(new URL("/api/message", location.origin), config);
    output.textContent = `Verified HTTP ${response.status}\n\n${new TextDecoder().decode(body)}\n\n${response.headers.get("signature-input")}`;
  } catch (error) {
    output.textContent = `REJECTED: ${error instanceof Error ? error.message : String(error)}`;
  }
});
