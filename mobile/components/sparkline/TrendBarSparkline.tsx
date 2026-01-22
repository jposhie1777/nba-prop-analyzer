// components/sparkline/TrendBarSparkline.tsx
import { BaseBarSparkline } from "./BaseBarSparkline";

export function TrendBarSparkline(props: any) {
  return (
    <BaseBarSparkline
      {...props}
      height={160}
      barWidth={20}
      barGap={16}
      baselineHeight={72}
      showValues
      dateStep="auto"
    />
  );
}