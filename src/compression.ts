import kafkajs, { CompressionTypes } from "kafkajs";
import SnappyCodec from "kafkajs-snappy";

// kafkajs's CJS->ESM interop doesn't statically detect CompressionCodecs as
// a named export (only some of its module.exports keys are detected), so
// it's pulled off the default/namespace export instead.
const { CompressionCodecs } = kafkajs;

type CompressibleType = Exclude<CompressionTypes, CompressionTypes.None>;

// Registers Snappy globally on kafkajs's shared codec map so it works out
// of the box as kafka-kit's default (see producer.ts). GZIP is already
// built into kafkajs; LZ4/ZSTD are not bundled here (no mature, dependency-
// light options) but can be plugged in via registerCompressionCodec.
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

export { CompressionTypes };

/**
 * Register an additional kafkajs compression codec, e.g. LZ4 via the
 * `kafkajs-lz4` package: `registerCompressionCodec(CompressionTypes.LZ4, new LZ4().codec)`.
 * Snappy is already registered; GZIP is built into kafkajs.
 *
 * @param type - The kafkajs CompressionTypes value the codec implements (not None).
 * @param codec - A codec factory matching kafkajs's `CompressionCodecs` shape.
 */
export function registerCompressionCodec(type: CompressibleType, codec: () => unknown): void {
  CompressionCodecs[type] = codec;
}
