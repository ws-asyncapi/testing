/**
 * Transport-parametrized conformance suite.
 *
 * The protocol contract — RPC, typed errors, events, rooms, server→client RPC,
 * streams, presence, history, auth, middleware, idempotency, the
 * state-survives-across-messages invariant, cross-node fan-out and
 * connection-state recovery — written ONCE and run against any transport via a
 * {@link ConformanceDriver}. Wire the same suite to the in-memory harness, the
 * Node adapter, and the Elysia adapter so every adapter's per-connection state
 * handling is verified identically (this is the class of bug that shipped in
 * adapter-elysia 0.1.0 — presence state dropped between handlers).
 *
 * ```ts
 * import { runConformance, harnessDriver } from "@ws-asyncapi/testing";
 * import { describe, it, expect } from "bun:test";
 * runConformance(harnessDriver, { describe, it, expect });
 * ```
 *
 * Adapter repos provide their own driver (a real server + the real client) and
 * may pass extra `codecs` / `backplanes` to widen the matrix.
 */
import type { WsClient } from "@ws-asyncapi/client";
import {
	type AnyChannel,
	type Backplane,
	Channel,
	type Codec,
	jsonCodec,
	LocalBackplane,
	RpcError,
	type ServerPlugin,
} from "ws-asyncapi";
import { z } from "zod";

// --- driver contract ---------------------------------------------------------

/** A running server + a way to connect clients to it and tear it down. */
export interface ConformanceServer {
	// biome-ignore lint/suspicious/noExplicitAny: channel-erased at the suite level
	connect(options?: {
		path?: string;
		query?: Record<string, string>;
		headers?: Record<string, string>;
	}): WsClient<any>;
	backplane: Backplane;
	close(): Promise<void>;
}

/** Stands a channel up on a concrete transport (harness / node / elysia). */
export interface ConformanceDriver {
	name: string;
	setup(
		channels: AnyChannel[],
		options: {
			codec?: Codec;
			backplane?: Backplane;
			plugins?: ServerPlugin[];
		},
	): Promise<ConformanceServer> | ConformanceServer;
	capabilities?: { crossNode?: boolean; recovery?: boolean };
}

/** A backplane variant in the matrix. `crossNode`/`recovery` gate the scenarios
 *  that need cluster fan-out or a replay log. */
export interface BackplaneVariant {
	name: string;
	create: () => Backplane;
	crossNode?: boolean;
	recovery?: boolean;
}

export interface ConformanceOptions {
	/** codec variants to run (default: just JSON). */
	codecs?: Array<[string, Codec]>;
	/** backplane variants to run (default: a recovery-capable LocalBackplane). */
	backplanes?: BackplaneVariant[];
	/** skip scenarios that need server-level plugins (some drivers may not wire them) */
	skipPlugins?: boolean;
}

/** Minimal `describe`/`it`/`expect` surface (pass `bun:test`'s). */
export interface TestDeps {
	// biome-ignore lint/suspicious/noExplicitAny: test runner shapes
	describe: (name: string, fn: () => void) => void;
	// biome-ignore lint/suspicious/noExplicitAny: test runner shapes
	it: (name: string, fn: () => any) => void;
	// biome-ignore lint/suspicious/noExplicitAny: test runner shapes
	expect: (value: any) => any;
}

// --- the conformance channel -------------------------------------------------

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Per-instance observable state (idempotency executions, plugin events). */
export interface ConformanceState {
	charges: number;
	pluginEvents: string[];
}

/** Build a fresh channel exercising the whole contract, plus its observable
 *  state. Fresh per scenario so counters don't leak between tests. */
export function makeConformanceChannel(state: ConformanceState) {
	const channel = new Channel("/room/:id", "room")
		.query(z.object({ name: z.string().optional() }))
		.onAuth(z.object({ token: z.string() }), ({ credentials }) => {
			if (credentials.token === "bad")
				throw new RpcError("FORBIDDEN", "bad token");
			return {
				role: credentials.token === "admin" ? "admin" : "user",
			};
		})
		.derive(({ request }) => ({
			room: `room:${request.params.id}`,
			role: "anon" as string,
		}))
		.onOpen(({ ws, data }) => {
			ws.subscribe(data.room);
		})
		.beforeMessage(({ type }) => {
			if (type === "blocked")
				throw new RpcError("FORBIDDEN", "blocked by middleware");
		})
		.serverMessage("message", z.object({ text: z.string() }))
		.clientMessage(
			"say",
			async ({ ws, message, data }) => {
				ws.publish(data.room, "message", { text: message.text });
			},
			z.object({ text: z.string() }),
		)
		.clientMessage(
			"askMe",
			async ({ ws }) => {
				const who = await ws.request("whoami", {});
				ws.send("message", { text: `you are ${who.id}` });
			},
			z.object({}),
		)
		.rpc(
			"add",
			z.object({ a: z.number(), b: z.number() }),
			z.object({ sum: z.number() }),
			async ({ message }) => ({ sum: message.a + message.b }),
		)
		.rpc(
			"forbidden",
			z.object({}),
			z.object({ ok: z.boolean() }),
			async () => {
				throw new RpcError("FORBIDDEN", "nope", { reason: "test" });
			},
			{ FORBIDDEN: z.object({ reason: z.string() }) },
		)
		.rpc("slow", z.object({}), z.object({ ok: z.boolean() }), async () => {
			await delay(200);
			return { ok: true };
		})
		.rpc(
			"charge",
			z.object({}),
			z.object({ count: z.number() }),
			async () => {
				state.charges++;
				return { count: state.charges };
			},
		)
		.rpc(
			"myRole",
			z.object({}),
			z.object({ role: z.string() }),
			// biome-ignore lint/suspicious/noExplicitAny: derived/auth context
			async ({ data }) => ({ role: (data as any).role ?? "anon" }),
		)
		.rpc("blocked", z.object({}), z.object({ ok: z.boolean() }), async () => ({
			ok: true,
		}))
		.serverRpc("whoami", z.object({}), z.object({ id: z.string() }))
		.stream(
			"count",
			z.object({ to: z.number() }),
			z.object({ n: z.number() }),
			async function* ({ message, signal }) {
				for (let i = 1; i <= message.to; i++) {
					if (signal.aborted) return;
					yield { n: i };
				}
			},
		)
		.presence(z.object({ name: z.string() }))
		.history("message", { keep: 50 });
	return channel;
}

// --- helpers -----------------------------------------------------------------

// biome-ignore lint/suspicious/noExplicitAny: erased client
function nextEvent<T>(client: any, name = "message"): Promise<T> {
	return new Promise((resolve) => {
		const off = client.onEvent(name, (d: T) => {
			off();
			resolve(d);
		});
	});
}

// --- the suite ---------------------------------------------------------------

export function runConformance(
	driver: ConformanceDriver,
	deps: TestDeps,
	options: ConformanceOptions = {},
): void {
	const { describe, it, expect } = deps;
	const codecs = options.codecs ?? [["json", jsonCodec]];
	const backplanes: BackplaneVariant[] = options.backplanes ?? [
		{
			name: "local",
			create: () => new LocalBackplane(),
			crossNode: false,
			recovery: true,
		},
	];
	const caps = driver.capabilities ?? {};

	for (const [codecName, codec] of codecs) {
		for (const variant of backplanes) {
			const label = `[${driver.name} · ${codecName} · ${variant.name}]`;

			// One server, one observable state, per scenario.
			const start = async (plugins?: ServerPlugin[]) => {
				const state: ConformanceState = { charges: 0, pluginEvents: [] };
				const channel = makeConformanceChannel(state);
				const server = await driver.setup([channel], {
					codec,
					backplane: variant.create(),
					plugins,
				});
				return { server, state };
			};

			describe(`conformance ${label}`, () => {
				it("opens (handshake) and answers an RPC", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						expect(c.connected).toBe(true);
						expect(await c.request("add", { a: 2, b: 3 })).toEqual({
							sum: 5,
						});
						c.close();
					} finally {
						await server.close();
					}
				});

				it("a thrown RpcError surfaces with typed data", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						const res = await c.safeRequest("forbidden", {});
						expect(res.data).toBeNull();
						expect(res.error).toMatchObject({
							code: "FORBIDDEN",
							data: { reason: "test" },
						});
						c.close();
					} finally {
						await server.close();
					}
				});

				it("rejects invalid input with VALIDATION", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						await expect(
							// @ts-expect-error wrong shape on purpose
							c.request("add", { a: "x", b: 1 }),
						).rejects.toMatchObject({ code: "VALIDATION" });
						c.close();
					} finally {
						await server.close();
					}
				});

				it("request times out when the handler is too slow", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						await expect(
							c.request("slow", {}, { timeout: 50 }),
						).rejects.toMatchObject({ code: "TIMEOUT" });
						c.close();
					} finally {
						await server.close();
					}
				});

				it("fans a command out to the room", async () => {
					const { server } = await start();
					try {
						const a = server.connect();
						const b = server.connect();
						await Promise.all([a.opened, b.opened]);
						const onB = nextEvent<{ text: string }>(b);
						a.call("say", { text: "hi" });
						expect(await onB).toEqual({ text: "hi" });
						a.close();
						b.close();
					} finally {
						await server.close();
					}
				});

				it("server→client RPC round-trips", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						c.onRequest("whoami", () => ({ id: "abc" }));
						const echoed = nextEvent<{ text: string }>(c);
						c.call("askMe", {});
						expect((await echoed).text).toBe("you are abc");
						c.close();
					} finally {
						await server.close();
					}
				});

				it("streams the full sequence", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						const got: number[] = [];
						for await (const v of c.stream("count", { to: 3 }))
							got.push((v as { n: number }).n);
						expect(got).toEqual([1, 2, 3]);
						c.close();
					} finally {
						await server.close();
					}
				});

				it("early break cancels the stream server-side", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						const got: number[] = [];
						for await (const v of c.stream("count", { to: 1000 })) {
							got.push((v as { n: number }).n);
							if (got.length === 2) break;
						}
						expect(got).toEqual([1, 2]);
						c.close();
					} finally {
						await server.close();
					}
				});

				it("presence: set joins the roster; leave drops it", async () => {
					const { server } = await start();
					try {
						const a = server.connect();
						const b = server.connect();
						await Promise.all([a.opened, b.opened]);
						await a.presence.set({ name: "Alice" });
						const seen = new Promise<Map<string, { name: string }>>(
							(resolve) => {
								b.presence.subscribe((m) => {
									if (m.size >= 2) resolve(m as never);
								});
							},
						);
						await b.presence.set({ name: "Bob" });
						const names = [...(await seen).values()]
							.map((m) => m.name)
							.sort();
						expect(names).toEqual(["Alice", "Bob"]);

						const dropped = new Promise<void>((resolve) => {
							b.presence.subscribe((m) => {
								if (m.size === 1) resolve();
							});
						});
						a.close();
						await dropped;
						b.close();
					} finally {
						await server.close();
					}
				});

				it("history returns retained events for a subscribed room", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						c.call("say", { text: "one" });
						await nextEvent(c);
						// history append happens after the event is delivered, and
						// on a networked backplane (Redis) the write lands shortly
						// after — poll briefly so the scenario is transport-robust.
						let entries: Array<{ data: { text: string } }> = [];
						for (let i = 0; i < 40; i++) {
							entries = (await c.history("room:1")) as Array<{
								data: { text: string };
							}>;
							if (entries.some((e) => e.data.text === "one")) break;
							await delay(25);
						}
						expect(entries.map((e) => e.data.text)).toContain("one");
						c.close();
					} finally {
						await server.close();
					}
				});

				// The generalized regression for the adapter-elysia 0.1.0 bug:
				// per-connection state (derived context, presence room) must
				// survive across many messages on one connection.
				it("INVARIANT: per-connection state survives across messages", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						expect(await c.request("add", { a: 1, b: 1 })).toEqual({
							sum: 2,
						});
						// presence must still work after an earlier message —
						// this is exactly what regressed in adapter-elysia 0.1.0
						await c.presence.set({ name: "Alice" });
						expect(c.presence.self).not.toBeNull();
						// and another message still works afterward
						expect(await c.request("add", { a: 2, b: 2 })).toEqual({
							sum: 4,
						});
						c.close();
					} finally {
						await server.close();
					}
				});

				it("middleware rejects a guarded rpc with the thrown error", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						await expect(
							c.request("blocked", {}),
						).rejects.toMatchObject({ code: "FORBIDDEN" });
						// a non-guarded rpc still works
						expect(await c.request("add", { a: 1, b: 0 })).toEqual({
							sum: 1,
						});
						c.close();
					} finally {
						await server.close();
					}
				});

				it("idempotency: a keyed rpc runs the handler once", async () => {
					const { server, state } = await start();
					try {
						const c = server.connect();
						await c.opened;
						const r1 = await c.request(
							"charge",
							{},
							{ idempotencyKey: "k1" },
						);
						const r2 = await c.request(
							"charge",
							{},
							{ idempotencyKey: "k1" },
						);
						expect(r1).toEqual(r2);
						expect(state.charges).toBe(1);
						// a different key runs again
						await c.request("charge", {}, { idempotencyKey: "k2" });
						expect(state.charges).toBe(2);
						c.close();
					} finally {
						await server.close();
					}
				});

				it("auth: authenticate refreshes the connection context", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						expect(
							((await c.request("myRole", {})) as { role: string })
								.role,
						).toBe("anon");
						await c.authenticate({ token: "admin" });
						expect(
							((await c.request("myRole", {})) as { role: string })
								.role,
						).toBe("admin");
						c.close();
					} finally {
						await server.close();
					}
				});

				it("auth: a rejected credential refresh throws", async () => {
					const { server } = await start();
					try {
						const c = server.connect();
						await c.opened;
						await expect(
							c.authenticate({ token: "bad" }),
						).rejects.toMatchObject({ code: "FORBIDDEN" });
						c.close();
					} finally {
						await server.close();
					}
				});

				if (!options.skipPlugins) {
					it("server plugins observe the lifecycle", async () => {
						const events: string[] = [];
						const plugin: ServerPlugin = {
							name: "probe",
							onConnection: () => events.push("conn"),
							onMessage: ({ name }) =>
								events.push(`msg:${name ?? "?"}`),
							onDisconnect: () => events.push("disc"),
						};
						const { server } = await start([plugin]);
						try {
							const c = server.connect();
							await c.opened;
							await c.request("add", { a: 1, b: 1 });
							c.close();
							// give the disconnect hook a tick
							await delay(50);
							expect(events).toContain("conn");
							expect(events.some((e) => e.startsWith("msg:"))).toBe(
								true,
							);
						} finally {
							await server.close();
						}
					});
				}

				if (variant.crossNode && caps.crossNode) {
					it("cross-node: an event on node A reaches a client on node B", async () => {
						const stateA: ConformanceState = {
							charges: 0,
							pluginEvents: [],
						};
						const stateB: ConformanceState = {
							charges: 0,
							pluginEvents: [],
						};
						const nodeA = await driver.setup(
							[makeConformanceChannel(stateA)],
							{ codec, backplane: variant.create() },
						);
						const nodeB = await driver.setup(
							[makeConformanceChannel(stateB)],
							{ codec, backplane: variant.create() },
						);
						try {
							const a = nodeA.connect();
							const b = nodeB.connect();
							await Promise.all([a.opened, b.opened]);
							const onA = nextEvent<{ text: string }>(a);
							b.call("say", { text: "cross" });
							expect((await onA).text).toBe("cross");
							a.close();
							b.close();
						} finally {
							await nodeA.close();
							await nodeB.close();
						}
					});
				}

				if (variant.recovery && caps.recovery) {
					it("recovery: a reconnecting client replays missed events", async () => {
						const { server } = await start();
						try {
							const a = server.connect();
							const b = server.connect();
							await Promise.all([a.opened, b.opened]);
							expect(a.sessionId).toBeTruthy();

							const missed = nextEvent<{ text: string }>(a);
							// drop A's underlying socket (not a clean client.close)
							// → the client reconnects with its sessionId.
							// biome-ignore lint/suspicious/noExplicitAny: raw socket
							(a as any)["~original"]?.close?.();
							// publish while A is down
							await delay(20);
							b.call("say", { text: "while-down" });

							expect((await missed).text).toBe("while-down");
							a.close();
							b.close();
						} finally {
							await server.close();
						}
					});
				}
			});
		}
	}
}
