import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
      '@/components': path.resolve(__dirname, './src/components'),
      '@/theme': path.resolve(__dirname, './src/theme'),
      '@/api': path.resolve(__dirname, './src/api'),
      '@/hooks': path.resolve(__dirname, './src/hooks'),
      '@/utils': path.resolve(__dirname, './src/utils'),
      '@/types': path.resolve(__dirname, './src/types'),
      '@/store': path.resolve(__dirname, './src/store'),
      '@/router': path.resolve(__dirname, './src/router'),
      '@/users': path.resolve(__dirname, './src/users'),
      '@/availability': path.resolve(__dirname, './src/availability'),
      '@/events': path.resolve(__dirname, './src/events'),
      '@/integrations': path.resolve(__dirname, './src/integrations'),
      '@/notifications': path.resolve(__dirname, './src/notifications'),
      '@/contacts': path.resolve(__dirname, './src/contacts'),
      '@/workflows': path.resolve(__dirname, './src/workflows'),
    },
  },
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
})