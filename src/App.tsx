import React from 'react';
import { ThemeProvider } from '@mui/material/styles';
import { CssBaseline } from '@mui/material';
import { QueryClientProvider } from '@tanstack/react-query';
import { ReactQueryDevtools } from '@tanstack/react-query-devtools';
import { RouterProvider } from 'react-router-dom';
import { ToastContainer } from 'react-toastify';
import { motion, AnimatePresence } from 'framer-motion';
import { theme } from '@/theme';
import { queryClient } from '@/api/queryClient';
import { router } from '@/router';
import { LoadingSpinner } from '@/components/core';

// Import toast styles
import 'react-toastify/dist/ReactToastify.css';

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        
        <AnimatePresence mode="wait">
          <motion.div
            key="app"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.3 }}
          >
            <React.Suspense fallback={<LoadingSpinner fullScreen message="Loading NovaMeet..." />}>
              <RouterProvider router={router} />
            </React.Suspense>
          </motion.div>
        </AnimatePresence>

        {/* Toast notifications */}
        <ToastContainer
          position="top-right"
          autoClose={5000}
          hideProgressBar={false}
          newestOnTop
          closeOnClick
          rtl={false}
          pauseOnFocusLoss
          draggable
          pauseOnHover
          theme="light"
          toastStyle={{
            borderRadius: '12px',
            fontFamily: theme.typography.fontFamily,
          }}
        />

        {/* React Query DevTools (only in development) */}
        {import.meta.env.DEV && <ReactQueryDevtools initialIsOpen={false} />}
      </ThemeProvider>
    </QueryClientProvider>
  );
}

export default App;