import { PaletteOptions } from '@mui/material/styles';

// Enterprise-grade color palette inspired by modern SaaS platforms
export const palette: PaletteOptions = {
  mode: 'light',
  primary: {
    main: '#6366F1', // Indigo - Professional and trustworthy
    light: '#8B8CF8',
    dark: '#4F46E5',
    contrastText: '#FFFFFF',
  },
  secondary: {
    main: '#EC4899', // Pink - Energetic accent
    light: '#F472B6',
    dark: '#DB2777',
    contrastText: '#FFFFFF',
  },
  error: {
    main: '#EF4444',
    light: '#F87171',
    dark: '#DC2626',
    contrastText: '#FFFFFF',
  },
  warning: {
    main: '#F59E0B',
    light: '#FBBF24',
    dark: '#D97706',
    contrastText: '#FFFFFF',
  },
  info: {
    main: '#3B82F6',
    light: '#60A5FA',
    dark: '#2563EB',
    contrastText: '#FFFFFF',
  },
  success: {
    main: '#10B981',
    light: '#34D399',
    dark: '#059669',
    contrastText: '#FFFFFF',
  },
  grey: {
    50: '#F9FAFB',
    100: '#F3F4F6',
    200: '#E5E7EB',
    300: '#D1D5DB',
    400: '#9CA3AF',
    500: '#6B7280',
    600: '#4B5563',
    700: '#374151',
    800: '#1F2937',
    900: '#111827',
  },
  background: {
    default: '#FAFBFC',
    paper: '#FFFFFF',
  },
  text: {
    primary: '#1F2937',
    secondary: '#6B7280',
    disabled: '#9CA3AF',
  },
  divider: '#E5E7EB',
  action: {
    active: '#6366F1',
    hover: 'rgba(99, 102, 241, 0.04)',
    selected: 'rgba(99, 102, 241, 0.08)',
    disabled: '#9CA3AF',
    disabledBackground: '#F3F4F6',
  },
};