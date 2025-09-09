import React from 'react';
import { Box, Toolbar } from '@mui/material';
import { motion } from 'framer-motion';
import { Header } from '../Header';
import { Sidebar } from '../Sidebar';
import { useUIStore } from '@/store/uiStore';

interface LayoutProps {
  children: React.ReactNode;
}

export const Layout: React.FC<LayoutProps> = ({ children }) => {
  const { sidebarCollapsed } = useUIStore();
  
  const DRAWER_WIDTH = sidebarCollapsed ? 72 : 280;

  return (
    <Box sx={{ display: 'flex' }}>
      <Header />
      <Sidebar />
      
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          p: 3,
          width: { sm: `calc(100% - ${DRAWER_WIDTH}px)` },
          ml: { sm: `${DRAWER_WIDTH}px` },
          transition: (theme) => theme.transitions.create(['margin', 'width'], {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.leavingScreen,
          }),
        }}
      >
        <Toolbar /> {/* Spacer for fixed header */}
        
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, ease: 'easeOut' }}
        >
          {children}
        </motion.div>
      </Box>
    </Box>
  );
};