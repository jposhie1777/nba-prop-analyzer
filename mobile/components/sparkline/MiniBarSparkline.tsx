// components/sparkline/MiniBarSparkline.tsx
import { BaseBarSparkline } from "./BaseBarSparkline";

type Props = {
  data?: number[];
  dates?: string[];
};

export function MiniBarSparkline({ data, dates }: Props) {
  return (
    <BaseBarSparkline
      data={data}
      dates={dates}
      height={64}
      barWidth={8}
      barGap={6}
      baselineHeight={44}
      showValues
      valueOffset={-18}
      showDates={false}
    />
  );
}
