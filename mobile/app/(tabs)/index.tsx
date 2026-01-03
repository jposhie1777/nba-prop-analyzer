import { View, ScrollView, Text } from "react-native";
import { useMemo, useState } from "react";
import PropCard from "../../components/PropCard";
import colors from "../../theme/color";
import { MOCK_PROPS } from "../../data/props";

export default function HomeScreen() {
  return (
    <View style={{ flex: 1, backgroundColor: colors.bg }}>
      <ScrollView showsVerticalScrollIndicator={false}>
        {MOCK_PROPS.map((prop) => (
          <PropCard key={prop.id} {...prop} />
        ))}
      </ScrollView>
    </View>
  );
}