import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/chat': {
        target: 'http://127.0.0.1:3000',
        changeOrigin: true,
      },
      '/agents': {
        target: 'http://127.0.0.1:3000',
        changeOrigin: true,
      },
      '/capabilities': {
        target: 'http://127.0.0.1:3000',
        changeOrigin: true,
      },
      '/sessions': {
        target: 'http://127.0.0.1:3000',
        changeOrigin: true,
      },
    },
  },
})