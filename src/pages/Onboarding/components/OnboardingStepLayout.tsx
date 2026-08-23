import type { FC, ReactNode } from "react";
import { cn } from "@/utils/cn";
import StepHero from "./StepHero";

interface OnboardingStepLayoutProps {
  children: ReactNode;
  contentClassName?: string;
  description: ReactNode;
  icon: ReactNode;
  iconClassName?: string;
  title: ReactNode;
}

const OnboardingStepLayout: FC<OnboardingStepLayoutProps> = (props) => {
  const {
    children,
    contentClassName,
    description,
    icon,
    iconClassName,
    title,
  } = props;

  return (
    <div className="flex min-h-0 flex-1 flex-col justify-center px-0 sm:px-10">
      <StepHero
        description={description}
        icon={icon}
        iconClassName={iconClassName}
        title={title}
      />

      <div
        className={cn("mx-auto mt-4 min-h-0 w-full sm:mt-8", contentClassName)}
      >
        {children}
      </div>
    </div>
  );
};

export default OnboardingStepLayout;
