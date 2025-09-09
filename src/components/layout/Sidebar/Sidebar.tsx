import React from 'react';
import {
  Drawer,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Box,
  Typography,
  Divider,
  Collapse,
  IconButton,
  useTheme,
} from '@mui/material';
import {
  Dashboard,
  Event,
  Schedule,
  Notifications,
  Contacts,
  AccountTree,
  Settings,
  ExpandLess,
  ExpandMore,
  ChevronLeft,
  ChevronRight,
} from '@mui/icons-material';

import IntegrationInstructionsIcon from '@mui/icons-material/IntegrationInstructions';
import { motion, AnimatePresence } from 'framer-motion';
import { useLocation, useNavigate } from 'react-router-dom';
import { useUIStore } from '@/store/uiStore';
import { useAuthStore } from '@/store/authStore';

const DRAWER_WIDTH = 280;
const DRAWER_WIDTH_COLLAPSED = 72;

interface NavigationItem {
  id: string;
  label: string;
  path: string;
  icon: React.ComponentType;
  children?: NavigationItem[];
  permission?: string;
  badge?: string | number;
}

const navigationItems: NavigationItem[] = [
  {
    id: 'dashboard',
    label: 'Dashboard',
    path: '/dashboard',
    icon: Dashboard,
  },
  {
    id: 'events',
    label: 'Events',
    path: '/events',
    icon: Event,
    children: [
      { id: 'event-types', label: 'Event Types', path: '/events/types', icon: Event },
      { id: 'bookings', label: 'Bookings', path: '/events/bookings', icon: Event },
      { id: 'analytics', label: 'Analytics', path: '/events/analytics', icon: Event },
    ],
  },
  {
    id: 'availability',
    label: 'Availability',
    path: '/availability',
    icon: Schedule,
    children: [
      { id: 'rules', label: 'Rules', path: '/availability/rules', icon: Schedule },
      { id: 'overrides', label: 'Overrides', path: '/availability/overrides', icon: Schedule },
      { id: 'blocked-times', label: 'Blocked Times', path: '/availability/blocked', icon: Schedule },
    ],
  },
  {
    id: 'integrations',
    label: 'Integrations',
    path: '/integrations',
    icon: IntegrationInstructionsIcon ,
    children: [
      { id: 'calendar', label: 'Calendar', path: '/integrations/calendar', icon: IntegrationInstructionsIcon  },
      { id: 'video', label: 'Video', path: '/integrations/video', icon: IntegrationInstructionsIcon  },
      { id: 'webhooks', label: 'Webhooks', path: '/integrations/webhooks', icon: IntegrationInstructionsIcon  },
    ],
  },
  {
    id: 'notifications',
    label: 'Notifications',
    path: '/notifications',
    icon: Notifications,
  },
  {
    id: 'contacts',
    label: 'Contacts',
    path: '/contacts',
    icon: Contacts,
  },
  {
    id: 'workflows',
    label: 'Workflows',
    path: '/workflows',
    icon: AccountTree,
  },
  {
    id: 'settings',
    label: 'Settings',
    path: '/settings',
    icon: Settings,
  },
];

export const Sidebar: React.FC = () => {
  const theme = useTheme();
  const location = useLocation();
  const navigate = useNavigate();
  const { sidebarOpen, sidebarCollapsed, toggleSidebarCollapse } = useUIStore();
  const { user } = useAuthStore();
  
  const [expandedItems, setExpandedItems] = React.useState<string[]>([]);

  const handleItemClick = (item: NavigationItem) => {
    if (item.children) {
      if (sidebarCollapsed) {
        // If collapsed, expand sidebar first
        toggleSidebarCollapse();
      } else {
        // Toggle expansion
        setExpandedItems(prev =>
          prev.includes(item.id)
            ? prev.filter(id => id !== item.id)
            : [...prev, item.id]
        );
      }
    } else {
      navigate(item.path);
    }
  };

  const isItemActive = (item: NavigationItem): boolean => {
    if (item.children) {
      return item.children.some(child => location.pathname.startsWith(child.path));
    }
    return location.pathname === item.path || location.pathname.startsWith(item.path + '/');
  };

  const drawerWidth = sidebarCollapsed ? DRAWER_WIDTH_COLLAPSED : DRAWER_WIDTH;

  return (
    <Drawer
      variant="permanent"
      open={sidebarOpen}
      sx={{
        width: drawerWidth,
        flexShrink: 0,
        '& .MuiDrawer-paper': {
          width: drawerWidth,
          boxSizing: 'border-box',
          transition: theme.transitions.create('width', {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.enteringScreen,
          }),
          overflowX: 'hidden',
        },
      }}
    >
      {/* Header */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          p: 2,
          minHeight: 64,
        }}
      >
        <AnimatePresence>
          {!sidebarCollapsed && (
            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -20 }}
              transition={{ duration: 0.2 }}
            >
              <Typography variant="h6" component="div" sx={{ fontWeight: 700, color: 'primary.main' }}>
                NovaMeet
              </Typography>
            </motion.div>
          )}
        </AnimatePresence>
        
        <IconButton onClick={toggleSidebarCollapse} size="small">
          {sidebarCollapsed ? <ChevronRight /> : <ChevronLeft />}
        </IconButton>
      </Box>

      <Divider />

      {/* User Info */}
      {!sidebarCollapsed && user && (
        <Box sx={{ p: 2 }}>
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3, delay: 0.1 }}
          >
            <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
              {user.profile?.display_name || user.full_name}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {user.email}
            </Typography>
          </motion.div>
        </Box>
      )}

      <Divider />

      {/* Navigation */}
      <List sx={{ flexGrow: 1, p: 1 }}>
        {navigationItems.map((item, index) => (
          <motion.div
            key={item.id}
            initial={{ opacity: 0, x: -20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.3, delay: index * 0.05 }}
          >
            <ListItemButton
              onClick={() => handleItemClick(item)}
              selected={isItemActive(item)}
              sx={{
                borderRadius: 1,
                mb: 0.5,
                minHeight: 48,
                ...(sidebarCollapsed && {
                  justifyContent: 'center',
                  px: 2.5,
                }),
              }}
            >
              <ListItemIcon
                sx={{
                  minWidth: sidebarCollapsed ? 0 : 40,
                  color: isItemActive(item) ? 'primary.main' : 'text.secondary', // Keep this line
                }}
              >
                <item.icon />
              </ListItemIcon>
              
              {!sidebarCollapsed && (
                <>
                  <ListItemText
                    primary={item.label}
                    sx={{
                      '& .MuiListItemText-primary': {
                        fontWeight: isItemActive(item) ? 600 : 400,
                        fontSize: '0.875rem',
                      },
                    }}
                  />
                  
                  {item.children && (
                    expandedItems.includes(item.id) ? <ExpandLess /> : <ExpandMore />
                  )}
                  
                  {item.badge && (
                    <Box
                      sx={{
                        backgroundColor: 'error.main',
                        color: 'white',
                        borderRadius: '50%',
                        minWidth: 20,
                        height: 20,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        fontSize: '0.75rem',
                        fontWeight: 600,
                      }}
                    >
                      {item.badge}
                    </Box>
                  )}
                </>
              )}
            </ListItemButton>

            {/* Submenu */}
            {item.children && !sidebarCollapsed && (
              <Collapse in={expandedItems.includes(item.id)} timeout="auto" unmountOnExit>
                <List component="div" disablePadding>
                  {item.children.map((child) => (
                    <ListItemButton
                      key={child.id}
                      onClick={() => navigate(child.path)}
                      selected={location.pathname === child.path}
                      sx={{
                        pl: 4,
                        borderRadius: 1,
                        mb: 0.5,
                        ml: 1,
                        mr: 1,
                      }}
                    >
                      <ListItemIcon sx={{ minWidth: 32 }}>
                        <child.icon fontSize="small" />
                      </ListItemIcon>
                      <ListItemText
                        primary={child.label}
                        sx={{
                          '& .MuiListItemText-primary': {
                            fontSize: '0.8125rem',
                          },
                        }}
                      />
                    </ListItemButton>
                  ))}
                </List>
              </Collapse>
            )}
          </motion.div>
        ))}
      </List>
    </Drawer>
  );
};