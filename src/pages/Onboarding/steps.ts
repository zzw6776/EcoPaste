import { isAndroid } from "@/utils/is";
import DoneStep from "./components/DoneStep";
import IgnoreAppsStep from "./components/IgnoreAppsStep";
import LegacyImportStep from "./components/LegacyImportStep";
import PermissionsStep from "./components/PermissionsStep";
import ShortcutsStep from "./components/ShortcutsStep";
import WelcomeStep from "./components/WelcomeStep";
import type { OnboardingStepDefinition } from "./types";

const ALL_STEPS: OnboardingStepDefinition[] = [
  {
    component: WelcomeStep,
    icon: "i-lucide:sparkles",
    id: "welcome",
  },
  {
    component: PermissionsStep,
    icon: "i-lucide:shield-check",
    id: "permissions",
  },
  {
    component: ShortcutsStep,
    icon: "i-lucide:keyboard",
    id: "shortcuts",
  },
  {
    component: IgnoreAppsStep,
    icon: "i-lucide:ban",
    id: "ignoreApps",
  },
  {
    component: LegacyImportStep,
    icon: "i-lucide:database-backup",
    id: "legacyImport",
  },
  {
    component: DoneStep,
    icon: "i-lucide:badge-check",
    id: "done",
  },
];

export const ONBOARDING_STEPS: OnboardingStepDefinition[] = isAndroid
  ? ALL_STEPS.filter((step) => {
      return ["welcome", "permissions", "done"].includes(step.id);
    })
  : ALL_STEPS;
