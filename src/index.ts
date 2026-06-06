/**
 * In-memory test harness for ws-asyncapi.
 *
 * `createTestHarness(channel)` wires the channel to a typed client over an
 * in-memory pipe — no real sockets, no ports, no `listen()`. It runs the *real*
 * dispatcher and the real `WebSocketNode` implementation (only the byte transport
 * is swapped for a pair of linked in-memory sockets), so tests exercise the
 * actual protocol code: RPC, typed errors, events, rooms/broadcast, recovery,
 * server→client RPC, and streams all work exactly as in production.
 *
 * ```ts
 * const h = createTestHarness(chat);
 * const client = h.connect();
 * await client.opened;
 * expect(await client.request("history", { limit: 10 })).toEqual(...);
 * await h.close();
 * ```
 */
import { WebSocketNode, WsHub } from "@ws-asyncapi/adapter-node";
import { createClient, type WebSocketLike, type WsClient } from "@ws-asyncapi/client";
import {
    type AnyChannel,
    type AnyFrame,
    applyCommand,
    type Backplane,
    type Codec,
    closeConnection,
    COMMAND_TOPIC,
    type Connection,
    dispatchFrame,
    type InferClient,
    jsonCodec,
    LocalBackplane,
    type NodeCommand,
    openConnection,
    OutboundRpc,
    publishEvent,
    type ServerPlugin,
    StreamRegistry,
} from "ws-asyncapi";

export interface TestHarnessOptions {
    /** wire codec (default: JSON). Client and server share it automatically. */
    codec?: Codec;
    /** backplane (default: a fresh in-process LocalBackplane with recovery on). */
    backplane?: Backplane;
    /** server-level plugins (metrics/tracing/logging) tapping the channel */
    plugins?: ServerPlugin[];
}

export interface TestConnectOptions {
    /** connection path; defaults to the channel address with params filled `1` */
    path?: string;
    query?: Record<string, string>;
    headers?: Record<string, string>;
}

export interface TestHarness<C extends AnyChannel> {
    /** the channel under test */
    channel: C;
    /** the backplane the harness drives (publish to it to simulate other nodes) */
    backplane: Backplane;
    /** open a typed client connected in-memory to the channel */
    connect(options?: TestConnectOptions): WsClient<InferClient<C>>;
    /** disconnect all clients and close the backplane */
    close(): Promise<void>;
}

/** Extract `:param` values from a concrete path against the channel address. */
function extractParams(
    address: string,
    path: string,
): Record<string, string> {
    const a = address.split("/").filter(Boolean);
    const p = path.split("/").filter(Boolean);
    const params: Record<string, string> = {};
    for (let i = 0; i < a.length; i++)
        if (a[i].startsWith(":")) params[a[i].slice(1)] = p[i] ?? "";
    return params;
}

/** Client end of the in-memory pipe: a minimal {@link WebSocketLike}. */
class MemoryClientSocket implements WebSocketLike {
    binaryType = "arraybuffer";
    readyState = 1; // OPEN — the pipe is live; onopen is fired explicitly
    onopen: ((event: unknown) => void) | null = null;
    onmessage: ((event: { data: unknown }) => void) | null = null;
    onerror: ((event: unknown) => void) | null = null;
    onclose: ((event: unknown) => void) | null = null;

    constructor(
        private toServer: (data: string | Uint8Array) => void,
        private onClientClose: () => void,
    ) {}

    send(data: string | Uint8Array): void {
        if (this.readyState === 1) this.toServer(data);
    }
    close(code?: number, reason?: string): void {
        if (this.readyState === 3) return;
        this.readyState = 3;
        this.onClientClose();
        queueMicrotask(() =>
            this.onclose?.({ code: code ?? 1000, reason: reason ?? "" }),
        );
    }
    /** server → client */
    deliver(data: string | Uint8Array): void {
        queueMicrotask(() => this.onmessage?.({ data }));
    }
    /** handshake/openConnection finished → fire onopen */
    fireOpen(): void {
        queueMicrotask(() => this.onopen?.({}));
    }
    /** the server closed this socket */
    serverClosed(code?: number, reason?: string): void {
        if (this.readyState === 3) return;
        this.readyState = 3;
        queueMicrotask(() =>
            this.onclose?.({ code: code ?? 1000, reason: reason ?? "" }),
        );
    }
}

export function createTestHarness<C extends AnyChannel>(
    channel: C,
    options: TestHarnessOptions = {},
): TestHarness<C> {
    const codec = options.codec ?? jsonCodec;
    const backplane = options.backplane ?? new LocalBackplane();
    const hub = new WsHub();
    const channelsByName = new Map([[channel.name, channel]]);
    const clients: MemoryClientSocket[] = [];

    // deliver backplane messages to local sockets (mirror of the adapters)
    backplane.onMessage((message) => {
        if (message.topic === COMMAND_TOPIC) {
            let cmd: NodeCommand | null = null;
            try {
                cmd = JSON.parse(
                    typeof message.payload === "string"
                        ? message.payload
                        : new TextDecoder().decode(message.payload),
                ) as NodeCommand;
            } catch {}
            if (cmd)
                applyCommand(
                    channelsByName.get(cmd.channel),
                    cmd,
                    message.origin === backplane.nodeId,
                );
            return;
        }
        hub.localPublish(message.topic, message.payload, message.except);
    });

    channel["~"].globalPublish = (topic: string, type: string, data: unknown) =>
        void publishEvent(backplane, codec, topic, type, data);
    channel["~"].fetchSockets = async (room?: string) => {
        const ids = room ? await backplane.roomMembers(room) : hub.ids();
        return Promise.all(
            ids.map(async (id) => ({
                id,
                rooms: (await backplane.rooms(id)).filter(
                    (r) => !r.startsWith("#sid:"),
                ),
            })),
        );
    };
    channel["~"].sendCommand = (cmd: NodeCommand) =>
        void backplane.publish(COMMAND_TOPIC, JSON.stringify(cmd));
    if (options.plugins) channel["~"].serverPlugins = options.plugins;
    channel["~"].publishFrame = (topic, frame, except) =>
        void backplane.publish(topic, codec.encode(frame), undefined, except);
    if (channel["~"].history.size)
        backplane.configureHistory?.(Object.fromEntries(channel["~"].history));

    function connect(connOpts: TestConnectOptions = {}): WsClient<InferClient<C>> {
        const path =
            connOpts.path ?? channel.address.replace(/:([^/]+)/g, "1");
        const params = extractParams(channel.address, path);
        const id = crypto.randomUUID();

        // server end: only send/readyState/close are used by WebSocketNode/WsHub
        const serverSock = {
            readyState: 1,
            send: (data: string | Uint8Array) => clientSock.deliver(data),
            close: (code?: number, reason?: string) =>
                clientSock.serverClosed(code, reason),
        };
        hub.add(id, serverSock as never);

        const outbound = new OutboundRpc();
        const conn: Connection = {
            // biome-ignore lint/suspicious/noExplicitAny: in-memory ws shim
            ws: new WebSocketNode<any, any>(
                serverSock as never,
                id,
                hub,
                codec,
                backplane,
                outbound,
            ),
            request: {
                query: connOpts.query ?? {},
                headers: connOpts.headers ?? {},
                params,
            },
            data: {},
            outbound,
            streams: new StreamRegistry(),
        };

        const clientSock = new MemoryClientSocket(
            (data) => {
                let frame: AnyFrame;
                try {
                    frame = codec.decode(data);
                } catch {
                    return;
                }
                void dispatchFrame(channel, backplane, conn, frame);
            },
            () => {
                serverSock.readyState = 3;
                void closeConnection(channel, backplane, conn);
                hub.remove(id);
            },
        );
        clients.push(clientSock);

        // run derives/onOpen, then signal the client the connection is open
        void openConnection(channel, conn).then(() => clientSock.fireOpen());

        return createClient<C>(
            `ws://test${path}`,
            path as InferClient<C>["address"],
            {
                reconnect: false,
                heartbeat: false,
                query: connOpts.query as never,
                headers: connOpts.headers as never,
                socket: () => clientSock,
            },
        );
    }

    return {
        channel,
        backplane,
        connect,
        close: async () => {
            for (const c of clients) c.close();
            await backplane.close();
        },
    };
}
