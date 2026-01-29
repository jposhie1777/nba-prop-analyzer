module.exports = function (api) {
  api.cache.forever(); // Changed from api.cache(true)
  
  return {
    presets: ["babel-preset-expo"],
    plugins: [
      "expo-router/babel",
      "react-native-reanimated/plugin",
    ],
  };
};