import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { User } from '@/types';

interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  
  // Actions
  setUser: (user: User) => void;
  setToken: (token: string) => void;
  login: (user: User, token: string) => void;
  logout: () => void;
  setLoading: (loading: boolean) => void;
  updateUser: (updates: Partial<User>) => void;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      user: null,
      token: null,
      isAuthenticated: false,
      isLoading: false,
      
      setUser: (user) => set({ user, isAuthenticated: !!user }),
      
      setToken: (token) => {
        localStorage.setItem('auth_token', token);
        set({ token });
      },
      
      login: (user, token) => {
        localStorage.setItem('auth_token', token);
        set({ 
          user, 
          token, 
          isAuthenticated: true,
          isLoading: false 
        });
      },
      
      logout: () => {
        localStorage.removeItem('auth_token');
        set({ 
          user: null, 
          token: null, 
          isAuthenticated: false,
          isLoading: false 
        });
      },
      
      setLoading: (isLoading) => set({ isLoading }),
      
      updateUser: (updates) => {
        const currentUser = get().user;
        if (currentUser) {
          set({ user: { ...currentUser, ...updates } });
        }
      },
    }),
    {
      name: 'novameet-auth',
      partialize: (state) => ({ 
        user: state.user, 
        token: state.token,
        isAuthenticated: state.isAuthenticated 
      }),
    }
  )
);