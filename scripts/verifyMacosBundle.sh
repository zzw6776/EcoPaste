#!/usr/bin/env bash
set -euo pipefail

# 校验 App 资源封装、稳定签名身份及 DMG 内的实际内容；自签名版本不经过 Apple 公证。
bundle_dir="${1:?Usage: verifyMacosBundle.sh <bundle-directory>}"
app_path="${bundle_dir}/macos/EcoPaste.app"
codesign --verify --deep --strict --verbose=2 "${app_path}"

signature_details="$(codesign --display --verbose=4 "${app_path}" 2>&1)"
if grep -Fq 'Signature=adhoc' <<< "${signature_details}"; then
  echo "Expected a stable macOS signing identity, found an ad-hoc signature." >&2
  exit 1
fi
if ! grep -Fxq 'Authority=Mac Development: EcoPaste Self-Signed' <<< "${signature_details}"; then
  echo "Expected signing authority 'Mac Development: EcoPaste Self-Signed'." >&2
  exit 1
fi

designated_requirement="$(codesign --display --requirements - "${app_path}" 2>&1)"
if grep -Fq 'cdhash ' <<< "${designated_requirement}"; then
  echo "Designated requirement is tied to one app build instead of the stable certificate." >&2
  exit 1
fi
if ! grep -Fq 'identifier "com.ayangweb.eco-paste"' <<< "${designated_requirement}"; then
  echo "Designated requirement does not contain the EcoPaste bundle identifier." >&2
  exit 1
fi

shopt -s nullglob
dmg_paths=("${bundle_dir}/dmg/"*.dmg)
if [[ ${#dmg_paths[@]} -ne 1 ]]; then
  echo "Expected exactly one macOS DMG, found ${#dmg_paths[@]}." >&2
  exit 1
fi

hdiutil verify "${dmg_paths[0]}"

mount_dir="$(mktemp -d "${TMPDIR:-/tmp}/ecopaste-dmg-verify.XXXXXX")"
mounted=false

# 只卸载本次校验挂载的镜像，校验失败也要释放挂载点。
cleanup() {
  if [[ "${mounted}" == true ]]; then
    hdiutil detach "${mount_dir}"
  fi
  rmdir "${mount_dir}"
}
trap cleanup EXIT

# DMG 内嵌本项目的 LICENSE，非交互式挂载需要确认许可提示。
hdiutil attach -readonly -nobrowse -noautoopen \
  -mountpoint "${mount_dir}" "${dmg_paths[0]}" <<< 'yes'
mounted=true

codesign --verify --deep --strict --verbose=2 "${mount_dir}/EcoPaste.app"
diff -qr "${app_path}" "${mount_dir}/EcoPaste.app"
