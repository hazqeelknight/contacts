// vite.config.ts
import { defineConfig } from "file:///home/project/node_modules/vite/dist/node/index.js";
import react from "file:///home/project/node_modules/@vitejs/plugin-react/dist/index.js";
import path from "path";
var __vite_injected_original_dirname = "/home/project";
var vite_config_default = defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__vite_injected_original_dirname, "./src"),
      "@/components": path.resolve(__vite_injected_original_dirname, "./src/components"),
      "@/theme": path.resolve(__vite_injected_original_dirname, "./src/theme"),
      "@/api": path.resolve(__vite_injected_original_dirname, "./src/api"),
      "@/hooks": path.resolve(__vite_injected_original_dirname, "./src/hooks"),
      "@/utils": path.resolve(__vite_injected_original_dirname, "./src/utils"),
      "@/types": path.resolve(__vite_injected_original_dirname, "./src/types"),
      "@/store": path.resolve(__vite_injected_original_dirname, "./src/store"),
      "@/router": path.resolve(__vite_injected_original_dirname, "./src/router"),
      "@/users": path.resolve(__vite_injected_original_dirname, "./src/users"),
      "@/availability": path.resolve(__vite_injected_original_dirname, "./src/availability"),
      "@/events": path.resolve(__vite_injected_original_dirname, "./src/events"),
      "@/integrations": path.resolve(__vite_injected_original_dirname, "./src/integrations"),
      "@/notifications": path.resolve(__vite_injected_original_dirname, "./src/notifications"),
      "@/contacts": path.resolve(__vite_injected_original_dirname, "./src/contacts"),
      "@/workflows": path.resolve(__vite_injected_original_dirname, "./src/workflows")
    }
  },
  server: {
    port: 3e3,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true
      }
    }
  },
  build: {
    outDir: "dist",
    sourcemap: true
  }
});
export {
  vite_config_default as default
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsidml0ZS5jb25maWcudHMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLnRzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcudHNcIjtpbXBvcnQgeyBkZWZpbmVDb25maWcgfSBmcm9tICd2aXRlJ1xuaW1wb3J0IHJlYWN0IGZyb20gJ0B2aXRlanMvcGx1Z2luLXJlYWN0J1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCdcblxuLy8gaHR0cHM6Ly92aXRlanMuZGV2L2NvbmZpZy9cbmV4cG9ydCBkZWZhdWx0IGRlZmluZUNvbmZpZyh7XG4gIHBsdWdpbnM6IFtyZWFjdCgpXSxcbiAgcmVzb2x2ZToge1xuICAgIGFsaWFzOiB7XG4gICAgICAnQCc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYycpLFxuICAgICAgJ0AvY29tcG9uZW50cyc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYy9jb21wb25lbnRzJyksXG4gICAgICAnQC90aGVtZSc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYy90aGVtZScpLFxuICAgICAgJ0AvYXBpJzogcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4vc3JjL2FwaScpLFxuICAgICAgJ0AvaG9va3MnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMvaG9va3MnKSxcbiAgICAgICdAL3V0aWxzJzogcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4vc3JjL3V0aWxzJyksXG4gICAgICAnQC90eXBlcyc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYy90eXBlcycpLFxuICAgICAgJ0Avc3RvcmUnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMvc3RvcmUnKSxcbiAgICAgICdAL3JvdXRlcic6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYy9yb3V0ZXInKSxcbiAgICAgICdAL3VzZXJzJzogcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4vc3JjL3VzZXJzJyksXG4gICAgICAnQC9hdmFpbGFiaWxpdHknOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMvYXZhaWxhYmlsaXR5JyksXG4gICAgICAnQC9ldmVudHMnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMvZXZlbnRzJyksXG4gICAgICAnQC9pbnRlZ3JhdGlvbnMnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMvaW50ZWdyYXRpb25zJyksXG4gICAgICAnQC9ub3RpZmljYXRpb25zJzogcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4vc3JjL25vdGlmaWNhdGlvbnMnKSxcbiAgICAgICdAL2NvbnRhY3RzJzogcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4vc3JjL2NvbnRhY3RzJyksXG4gICAgICAnQC93b3JrZmxvd3MnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMvd29ya2Zsb3dzJyksXG4gICAgfSxcbiAgfSxcbiAgc2VydmVyOiB7XG4gICAgcG9ydDogMzAwMCxcbiAgICBwcm94eToge1xuICAgICAgJy9hcGknOiB7XG4gICAgICAgIHRhcmdldDogJ2h0dHA6Ly9sb2NhbGhvc3Q6ODAwMCcsXG4gICAgICAgIGNoYW5nZU9yaWdpbjogdHJ1ZSxcbiAgICAgIH0sXG4gICAgfSxcbiAgfSxcbiAgYnVpbGQ6IHtcbiAgICBvdXREaXI6ICdkaXN0JyxcbiAgICBzb3VyY2VtYXA6IHRydWUsXG4gIH0sXG59KSJdLAogICJtYXBwaW5ncyI6ICI7QUFBeU4sU0FBUyxvQkFBb0I7QUFDdFAsT0FBTyxXQUFXO0FBQ2xCLE9BQU8sVUFBVTtBQUZqQixJQUFNLG1DQUFtQztBQUt6QyxJQUFPLHNCQUFRLGFBQWE7QUFBQSxFQUMxQixTQUFTLENBQUMsTUFBTSxDQUFDO0FBQUEsRUFDakIsU0FBUztBQUFBLElBQ1AsT0FBTztBQUFBLE1BQ0wsS0FBSyxLQUFLLFFBQVEsa0NBQVcsT0FBTztBQUFBLE1BQ3BDLGdCQUFnQixLQUFLLFFBQVEsa0NBQVcsa0JBQWtCO0FBQUEsTUFDMUQsV0FBVyxLQUFLLFFBQVEsa0NBQVcsYUFBYTtBQUFBLE1BQ2hELFNBQVMsS0FBSyxRQUFRLGtDQUFXLFdBQVc7QUFBQSxNQUM1QyxXQUFXLEtBQUssUUFBUSxrQ0FBVyxhQUFhO0FBQUEsTUFDaEQsV0FBVyxLQUFLLFFBQVEsa0NBQVcsYUFBYTtBQUFBLE1BQ2hELFdBQVcsS0FBSyxRQUFRLGtDQUFXLGFBQWE7QUFBQSxNQUNoRCxXQUFXLEtBQUssUUFBUSxrQ0FBVyxhQUFhO0FBQUEsTUFDaEQsWUFBWSxLQUFLLFFBQVEsa0NBQVcsY0FBYztBQUFBLE1BQ2xELFdBQVcsS0FBSyxRQUFRLGtDQUFXLGFBQWE7QUFBQSxNQUNoRCxrQkFBa0IsS0FBSyxRQUFRLGtDQUFXLG9CQUFvQjtBQUFBLE1BQzlELFlBQVksS0FBSyxRQUFRLGtDQUFXLGNBQWM7QUFBQSxNQUNsRCxrQkFBa0IsS0FBSyxRQUFRLGtDQUFXLG9CQUFvQjtBQUFBLE1BQzlELG1CQUFtQixLQUFLLFFBQVEsa0NBQVcscUJBQXFCO0FBQUEsTUFDaEUsY0FBYyxLQUFLLFFBQVEsa0NBQVcsZ0JBQWdCO0FBQUEsTUFDdEQsZUFBZSxLQUFLLFFBQVEsa0NBQVcsaUJBQWlCO0FBQUEsSUFDMUQ7QUFBQSxFQUNGO0FBQUEsRUFDQSxRQUFRO0FBQUEsSUFDTixNQUFNO0FBQUEsSUFDTixPQUFPO0FBQUEsTUFDTCxRQUFRO0FBQUEsUUFDTixRQUFRO0FBQUEsUUFDUixjQUFjO0FBQUEsTUFDaEI7QUFBQSxJQUNGO0FBQUEsRUFDRjtBQUFBLEVBQ0EsT0FBTztBQUFBLElBQ0wsUUFBUTtBQUFBLElBQ1IsV0FBVztBQUFBLEVBQ2I7QUFDRixDQUFDOyIsCiAgIm5hbWVzIjogW10KfQo=
