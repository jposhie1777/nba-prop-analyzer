import { ScrollView } from "react-native";
import { BarSparkline } from "@/components/BarSparkline";

type Props = {
  values: number[];
  dates: string[];
};

export function TrendBarChart({ values, dates }: Props) {
  return (
    <ScrollView
      horizontal
      showsHorizontalScrollIndicator={false}
      contentContainerStyle={{
        paddingHorizontal: 16,
        paddingVertical: 8,
      }}
    >
      <BarSparkline
        data={values}
        dates={dates}
        height={160}
      />
    </ScrollView>
  );
}