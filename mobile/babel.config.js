module.exports = function (api) {
  api.cache(true);
  return {
    presets: ["babel-preset-expo"],
    plugins: [
      "expo-router/babel",          // 👈 REQUIRED
      "react-native-reanimated/plugin", // 👈 MUST BE LAST
    ],
  };
};