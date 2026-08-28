import {
  defineConfig,
  presetIcons,
  presetWind4,
  transformerDirectives,
  transformerVariantGroup,
} from "unocss";
import { presetAntdColors } from "./src/unocss/presetAntdColors";

export default defineConfig({
  presets: [
    presetWind4({
      // On-demand theme tracking follows parallel scan order and makes release asset hashes unstable.
      preflights: { theme: true },
    }),
    presetAntdColors(),
    presetIcons(),
  ],
  transformers: [
    transformerVariantGroup(),
    transformerDirectives({
      applyVariable: ["--uno"],
    }),
  ],
});
