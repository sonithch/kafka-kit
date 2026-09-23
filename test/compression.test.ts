import { describe, it } from "node:test";
import assert from "node:assert/strict";
import kafkajs from "kafkajs";
import { CompressionTypes, registerCompressionCodec } from "../src/compression.js";

// See src/compression.ts: kafkajs's CJS->ESM interop doesn't statically
// detect CompressionCodecs as a named export.
const { CompressionCodecs } = kafkajs;

describe("compression", () => {
  it("registers Snappy out of the box (importing kafka-kit registers it as a side effect)", () => {
    assert.equal(typeof CompressionCodecs[CompressionTypes.Snappy], "function");
  });

  it("Snappy codec actually round-trips a payload", async () => {
    const codec = CompressionCodecs[CompressionTypes.Snappy]();
    const original = Buffer.from(JSON.stringify({ hello: "world", n: 42 }));

    const compressed = await codec.compress({ buffer: original });
    assert.ok(Buffer.isBuffer(compressed));
    assert.ok(compressed.length > 0);

    const decompressed = await codec.decompress(compressed);
    assert.deepEqual(decompressed, original);
  });

  it("registerCompressionCodec lets callers plug in an additional codec (e.g. LZ4)", () => {
    const fakeLz4Codec = () => ({
      compress: async (encoder: { buffer: Buffer }) => encoder.buffer,
      decompress: async (buf: Buffer) => buf,
    });

    registerCompressionCodec(CompressionTypes.LZ4, fakeLz4Codec);

    assert.equal(CompressionCodecs[CompressionTypes.LZ4], fakeLz4Codec);
  });
});
