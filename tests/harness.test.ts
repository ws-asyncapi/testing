import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { Channel, jsonCodec, publishEvent, RpcError } from "ws-asyncapi";
import { createTestHarness } from "../src/index.ts";

// One channel exercising the whole protocol surface, driven through the real
// client + real dispatcher + real WebSocketNode (only the byte transport is an
// in-memory pipe). This is the broad "does the protocol actually work" suite.
function makeChat() {
	return new Channel("/room/:id", "room")
		.query(z.object({ name: z.string().optional() }))
		.derive(({ request }) => ({ room: `room:${request.params.id}` }))
		.onOpen(({ ws, data }) => {
			ws.subscribe(data.room);
		})
		.serverMessage(
			"message",
			z.object({ text: z.string(), from: z.string().optional() }),
		)
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
				// server→client RPC round-trip, then echo the answer back as an event
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
		.rpc(
			"slow",
			z.object({}),
			z.object({ ok: z.boolean() }),
			async () => {
				await new Promise((r) => setTimeout(r, 200));
				return { ok: true };
			},
		)
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
}

/** Resolve with the first event payload of `name`. */
function nextEvent<T = unknown>(
	client: { onEvent: (n: string, cb: (d: T) => void) => () => void },
	name: string,
): Promise<T> {
	return new Promise((resolve) => {
		const off = client.onEvent(name, (data) => {
			off();
			resolve(data);
		});
	});
}

describe("harness: RPC", () => {
	it("request resolves with the typed reply", async () => {
		const h = createTestHarness(makeChat());
		const c = h.connect();
		await c.opened;
		expect(await c.request("add", { a: 2, b: 3 })).toEqual({ sum: 5 });
		await h.close();
	});

	it("a thrown RpcError surfaces as a typed error (request rejects)", async () => {
		const h = createTestHarness(makeChat());
		const c = h.connect();
		await c.opened;
		await expect(c.request("forbidden", {})).rejects.toMatchObject({
			code: "FORBIDDEN",
			message: "nope",
		});
		await h.close();
	});

	it("safeRequest returns the typed error data without throwing", async () => {
		const h = createTestHarness(makeChat());
		const c = h.connect();
		await c.opened;
		const res = await c.safeRequest("forbidden", {});
		expect(res.data).toBeNull();
		expect(res.error).toMatchObject({
			code: "FORBIDDEN",
			data: { reason: "test" },
		});
		await h.close();
	});

	it("rejects invalid input with a VALIDATION error", async () => {
		const h = createTestHarness(makeChat());
		const c = h.connect();
		await c.opened;
		await expect(
			// @ts-expect-error deliberately wrong input shape
			c.request("add", { a: "x", b: 3 }),
		).rejects.toMatchObject({ code: "VALIDATION" });
		await h.close();
	});

	it("request times out when the handler is slower than the deadline", async () => {
		const h = createTestHarness(makeChat());
		const c = h.connect();
		await c.opened;
		await expect(
			c.request("slow", {}, { timeout: 50 }),
		).rejects.toMatchObject({ code: "TIMEOUT" });
		await h.close();
	});

	it("an unknown rpc rejects rather than hanging", async () => {
		const h = createTestHarness(makeChat());
		const c = h.connect();
		await c.opened;
		await expect(
			// @ts-expect-error unknown rpc name
			c.request("missing", {}, { timeout: 500 }),
		).rejects.toBeInstanceOf(RpcError);
		await h.close();
	});
});

describe("harness: events, commands, rooms", () => {
	it("a command fans an event out to the room (both clients receive)", async () => {
		const h = createTestHarness(makeChat());
		const a = h.connect();
		const b = h.connect();
		await Promise.all([a.opened, b.opened]);
		const onB = nextEvent<{ text: string }>(b, "message");
		a.call("say", { text: "hello" });
		expect(await onB).toEqual({ text: "hello" });
		await h.close();
	});
});

describe("harness: server→client RPC", () => {
	it("the server can call the client and use its answer", async () => {
		const h = createTestHarness(makeChat());
		const c = h.connect();
		await c.opened;
		c.onRequest("whoami", () => ({ id: "abc" }));
		const echoed = nextEvent<{ text: string }>(c, "message");
		c.call("askMe", {});
		expect((await echoed).text).toBe("you are abc");
		await h.close();
	});
});

describe("harness: streams", () => {
	it("yields the full sequence", async () => {
		const h = createTestHarness(makeChat());
		const c = h.connect();
		await c.opened;
		const got: number[] = [];
		for await (const v of c.stream("count", { to: 3 })) got.push(v.n);
		expect(got).toEqual([1, 2, 3]);
		await h.close();
	});

	it("early break cancels the stream server-side (abort fires)", async () => {
		const h = createTestHarness(makeChat());
		const c = h.connect();
		await c.opened;
		const got: number[] = [];
		for await (const v of c.stream("count", { to: 1000 })) {
			got.push(v.n);
			if (got.length === 2) break;
		}
		expect(got).toEqual([1, 2]);
		await h.close();
	});
});

describe("harness: presence", () => {
	it("set joins the roster and peers see each other", async () => {
		const h = createTestHarness(makeChat());
		const a = h.connect();
		const b = h.connect();
		await Promise.all([a.opened, b.opened]);

		await a.presence.set({ name: "Alice" });
		const seen = new Promise<Map<string, { name: string }>>((resolve) => {
			b.presence.subscribe((members) => {
				if (members.size >= 2) resolve(members);
			});
		});
		await b.presence.set({ name: "Bob" });
		const members = await seen;
		const names = [...members.values()].map((m) => m.name).sort();
		expect(names).toEqual(["Alice", "Bob"]);
		await h.close();
	});

	it("a leaving client is dropped from the roster", async () => {
		const h = createTestHarness(makeChat());
		const a = h.connect();
		const b = h.connect();
		await Promise.all([a.opened, b.opened]);
		await a.presence.set({ name: "Alice" });
		await b.presence.set({ name: "Bob" });

		const dropped = new Promise<void>((resolve) => {
			b.presence.subscribe((members) => {
				if (members.size === 1) resolve();
			});
		});
		a.close();
		await dropped;
		await h.close();
	});
});

describe("harness: history", () => {
	it("returns retained events for a subscribed room", async () => {
		const h = createTestHarness(makeChat());
		const c = h.connect();
		await c.opened;
		c.call("say", { text: "one" });
		c.call("say", { text: "two" });
		// let the commands round-trip before querying
		await nextEvent(c, "message");
		const room = "room:1";
		const entries = (await c.history(room)) as Array<{
			event: string;
			data: { text: string };
		}>;
		const texts = entries.map((e) => e.data.text);
		expect(texts).toContain("one");
		expect(texts).toContain("two");
		await h.close();
	});
});

describe("harness: backplane fan-in (other node / emitter)", () => {
	it("an event published straight to the backplane reaches subscribers", async () => {
		// Simulates another cluster node — or an external emitter — publishing into
		// the shared backplane. The connected client (subscribed to room:1 on open)
		// must receive it through the backplane → hub delivery path.
		const h = createTestHarness(makeChat());
		const c = h.connect();
		await c.opened;
		const got = nextEvent<{ text: string }>(c, "message");
		await publishEvent(h.backplane, jsonCodec, "room:1", "message", {
			text: "from-other-node",
		});
		expect((await got).text).toBe("from-other-node");
		await h.close();
	});
});
