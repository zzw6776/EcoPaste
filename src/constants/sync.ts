/** 与 Rust `sync::identity` 中支持的配对码前缀同步维护。 */
const SYNC_PAIRING_CODE_PREFIXES = [
  "ecopaste-pair-v2:",
  "ecopaste-pair-v1:",
] as const;

/** 判断内容是否为当前版本可处理的 EcoPaste 配对码。 */
export function isSyncPairingCode(value: string): boolean {
  return SYNC_PAIRING_CODE_PREFIXES.some((prefix) => value.startsWith(prefix));
}
