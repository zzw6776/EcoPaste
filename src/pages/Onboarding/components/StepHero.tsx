import type { FC, ReactNode } from "react";
import { cn } from "@/utils/cn";

interface StepHeroProps {
  description: ReactNode;
  icon: ReactNode;
  iconClassName?: string;
  title: ReactNode;
}

const StepHero: FC<StepHeroProps> = (props) => {
  const { description, icon, iconClassName, title } = props;

  return (
    <header className="flex shrink-0 flex-col items-center text-center">
      <div
        className={cn(
          "mb-2 flex size-12 items-center justify-center overflow-hidden text-4xl text-ant-primary leading-none sm:mb-3",
          iconClassName,
        )}
      >
        {icon}
      </div>
      <h1 className="m-0 font-semibold text-ant-text text-xl leading-tight sm:text-2xl">
        {title}
      </h1>
      <p className="m-0 mt-1.5 text-ant-secondary text-xs leading-relaxed sm:mt-2 sm:text-sm">
        {description}
      </p>
    </header>
  );
};

export default StepHero;
