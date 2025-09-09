import { create } from 'zustand';

interface UIState {
  // Sidebar
  sidebarOpen: boolean;
  sidebarCollapsed: boolean;
  
  // Notifications
  notifications: Array<{
    id: string;
    type: 'success' | 'error' | 'warning' | 'info';
    title: string;
    message: string;
    timestamp: string;
    read: boolean;
  }>;
  
  // Loading states
  globalLoading: boolean;
  
  // Theme
  darkMode: boolean;
  
  // Actions
  setSidebarOpen: (open: boolean) => void;
  setSidebarCollapsed: (collapsed: boolean) => void;
  toggleSidebar: () => void;
  toggleSidebarCollapse: () => void;
  
  addNotification: (notification: Omit<UIState['notifications'][0], 'id' | 'timestamp' | 'read'>) => void;
  markNotificationRead: (id: string) => void;
  clearNotifications: () => void;
  
  setGlobalLoading: (loading: boolean) => void;
  setDarkMode: (darkMode: boolean) => void;
  toggleDarkMode: () => void;
}

export const useUIStore = create<UIState>((set, get) => ({
  // Initial state
  sidebarOpen: true,
  sidebarCollapsed: false,
  notifications: [],
  globalLoading: false,
  darkMode: false,
  
  // Sidebar actions
  setSidebarOpen: (open) => set({ sidebarOpen: open }),
  setSidebarCollapsed: (collapsed) => set({ sidebarCollapsed: collapsed }),
  toggleSidebar: () => set((state) => ({ sidebarOpen: !state.sidebarOpen })),
  toggleSidebarCollapse: () => set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
  
  // Notification actions
  addNotification: (notification) => {
    const newNotification = {
      ...notification,
      id: Date.now().toString(),
      timestamp: new Date().toISOString(),
      read: false,
    };
    set((state) => ({
      notifications: [newNotification, ...state.notifications].slice(0, 50), // Keep only last 50
    }));
  },
  
  markNotificationRead: (id) => {
    set((state) => ({
      notifications: state.notifications.map((notif) =>
        notif.id === id ? { ...notif, read: true } : notif
      ),
    }));
  },
  
  clearNotifications: () => set({ notifications: [] }),
  
  // Loading actions
  setGlobalLoading: (loading) => set({ globalLoading: loading }),
  
  // Theme actions
  setDarkMode: (darkMode) => set({ darkMode }),
  toggleDarkMode: () => set((state) => ({ darkMode: !state.darkMode })),
}));