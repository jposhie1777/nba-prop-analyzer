const { getDefaultConfig } = require("expo/metro-config");
const path = require("path");

const config = getDefaultConfig(__dirname);

// Fix Zustand import.meta error
config.resolver.extraNodeModules = {
  ...(config.resolver.extraNodeModules || {}),
  "zustand/middleware/devtools": path.resolve(__dirname, "lib/noopDevtools.ts"),
};

// CRITICAL FIX: Exclude .mjs files from being resolved
// This prevents Metro from loading Zustand's ESM version which has import.meta
config.resolver.sourceExts = config.resolver.sourceExts.filter(ext => ext !== 'mjs');

// Force Zustand to use CommonJS instead of ESM  
config.resolver.resolveRequest = (context, moduleName, platform) => {
  if (
    platform === 'web' && 
    (moduleName === 'zustand' || moduleName.startsWith('zustand/'))
  ) {
    const defaultResolve = context.resolveRequest;
    try {
      const result = defaultResolve(context, moduleName, platform);
      if (result && result.filePath && result.filePath.endsWith('.mjs')) {
        const jsPath = result.filePath.replace(/\.mjs$/, '.js');
        const fs = require('fs');
        if (fs.existsSync(jsPath)) {
          return { ...result, filePath: jsPath };
        }
      }
      return result;
    } catch (error) {
      // Fall through
    }
  }
  return context.resolveRequest(context, moduleName, platform);
};

// Transformer configuration
config.transformer = {
  ...config.transformer,
  getTransformOptions: async () => ({
    transform: {
      experimentalImportSupport: false,
      inlineRequires: true,
    },
  }),
};

module.exports = config;