import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In development the console runs on Vite's own server and the gateway on
// :8080, so every API call is proxied across. In production the gateway
// serves the built assets itself and nothing is proxied — which is why
// every request in the app is a same-origin relative path.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8080",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
  },
});
