import { View } from "react-native";
import Svg, { Polyline } from "react-native-svg";
import { useTheme } from "@/store/useTheme";

type Props = {
  data?: number[];
  width?: number;
  height?: number;
};

export function Sparkline({
  data,
  width = 90,
  height = 28,
}: Props) {
  const colors = useTheme((s) => s.colors);

  if (!data || data.length < 2) return null;

  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;

  const points = data
    .map((v, i) => {
      const x = (i / (data.length - 1)) * width;
      const y = height - ((v - min) / range) * height;
      return `${x},${y}`;
    })
    .join(" ");

  return (
    <View>
      <Svg width={width} height={height}>
        <Polyline
          points={points}
          fill="none"
          stroke={colors.accent.primary}
          strokeWidth={2}
          strokeLinejoin="round"
          strokeLinecap="round"
        />
      </Svg>
    </View>
  );
}