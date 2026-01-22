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
      height={48}
      barWidth={8}
      barGap={6}
      baselineHeight={32}
      showValues={false}
      showDates={false}
    />
  );
}