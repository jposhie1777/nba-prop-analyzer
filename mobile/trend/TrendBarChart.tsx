import { ScrollView } from "react-native";
import { TrendBarSparkline } from "@/components/sparkline/TrendBarSparkline";

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
      <TrendBarSparkline
        data={values}
        dates={dates}
      />
    </ScrollView>
  );
}