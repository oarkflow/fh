import { MiraBrowserClient } from "../../../browser/client.js";

const endpoint = document.querySelector<HTMLInputElement>("#endpoint")!;
const token = document.querySelector<HTMLInputElement>("#token")!;
const message = document.querySelector<HTMLInputElement>("#message")!;
const connectButton = document.querySelector<HTMLButtonElement>("#connect")!;
const sendButton = document.querySelector<HTMLButtonElement>("#send")!;
const closeButton = document.querySelector<HTMLButtonElement>("#close")!;
const output = document.querySelector<HTMLElement>("#log")!;
let client: MiraBrowserClient | undefined;

function log(text: string): void {
  output.textContent += `${new Date().toLocaleTimeString()} ${text}\n`;
}

connectButton.addEventListener("click", async () => {
  try {
    client = new MiraBrowserClient(endpoint.value, token.value, (event) => {
      log(`event cursor=${event.cursor}: ${new TextDecoder().decode(event.data)}`);
    });
    await client.connect();
    connectButton.disabled = true;
    sendButton.disabled = false;
    closeButton.disabled = false;
    log("connected");
  } catch (error) {
    log(`connect failed: ${String(error)}`);
    client = undefined;
  }
});

sendButton.addEventListener("click", async () => {
  if (!client) return;
  try {
    const id = await client.send(new TextEncoder().encode(message.value));
    log(`sent message id=${id}`);
  } catch (error) {
    log(`send failed: ${String(error)}`);
  }
});

closeButton.addEventListener("click", async () => {
  await client?.close();
  client = undefined;
  connectButton.disabled = false;
  sendButton.disabled = true;
  closeButton.disabled = true;
  log("closed");
});
