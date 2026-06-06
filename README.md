# @ws-asyncapi/testing

In-memory test harness for **ws-asyncapi**. Connect a fully-typed client to a
channel with **no sockets, no ports, no `listen()`** — the harness runs the real
dispatcher and the real `WebSocketNode` implementation over an in-memory pipe, so
your tests exercise the actual protocol: RPC, typed errors, events, rooms,
broadcast, server→client RPC, streams, and connection-state-recovery all behave
exactly as in production.

## Installation

```bash
npm install -D @ws-asyncapi/testing
# peers: ws-asyncapi, @ws-asyncapi/client, @ws-asyncapi/adapter-node
```

## Usage

```ts
import { describe, expect, test } from "bun:test"; // or vitest/jest
import { createTestHarness } from "@ws-asyncapi/testing";
import { chat } from "./server"; // your Channel

test("history RPC", async () => {
  const h = createTestHarness(chat);
  const client = h.connect();           // typed WsClient, inferred from `chat`
  await client.opened;

  const { items } = await client.request("history", { limit: 10 });
  expect(items).toHaveLength(10);

  await h.close();
});
```

Everything is typed straight from the channel — `client.request`, `onEvent`,
`call`, `safeRequest`, `stream`, `onRequest` all infer their argument and result
types, and wrong names/payloads are compile errors.

### Multiple clients (fan-out, presence)

```ts
const h = createTestHarness(chat);
const a = h.connect();
const b = h.connect();
await Promise.all([a.opened, b.opened]);

b.onEvent("message", (m) => received.push(m));
a.call("say", { text: "hi" });          // broadcasts to the room → b receives it
```

### Streams

```ts
for await (const tick of client.stream("prices", { symbol: "ACME" })) {
  // ...
}
```

### Simulating other cluster nodes

The harness drives a real backplane (a fresh `LocalBackplane` by default). Pass a
shared backplane to test cross-node behavior, or publish to `h.backplane`/use an
external emitter against it.

```ts
const h = createTestHarness(chat, { backplane, codec });
```

## API

- `createTestHarness(channel, options?)` → `{ channel, backplane, connect, close }`
  - `connect(options?)` → a typed client (`reconnect`/`heartbeat` are off by
    default; pass `path`, `query`, `headers` to override).
  - `close()` → disconnect all clients and close the backplane.
- `options`: `codec` (default JSON), `backplane` (default in-process `LocalBackplane`).

## License

MIT
