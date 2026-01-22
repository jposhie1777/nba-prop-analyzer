// components/sparkline/MiniBarSparkline.tsx
import { BaseBarSparkline } from "./BaseBarSparkline";

export function MiniBarSparkline(props: any) {
  return (
    <BaseBarSparkline
      {...props}
      height={48}
      barWidth={8}
      barGap={6}
      baselineHeight={32}
      showValues={false}
      dateStep={0}
    />
  );
}