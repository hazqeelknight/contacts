import { createTheme, ThemeOptions } from '@mui/material/styles';
import { palette } from './palette';
import { typography } from './typography';
import { components } from './components';
import { shadows } from './shadows';
import { spacing } from './spacing';

declare module '@mui/material/styles' {
  interface Theme {
    customShadows: {
      card: string;
      dropdown: string;
      modal: string;
      tooltip: string;
    };
  }
  
  interface ThemeOptions {
    customShadows?: {
      card?: string;
      dropdown?: string;
      modal?: string;
      tooltip?: string;
    };
  }
}

const themeOptions: ThemeOptions = {
  palette,
  typography,
  spacing,
  shape: {
    borderRadius: 12,
  },
  shadows,
  customShadows: {
    card: '0 2px 16px 0 rgba(145, 158, 171, 0.08)',
    dropdown: '0 8px 32px 0 rgba(145, 158, 171, 0.12)',
    modal: '0 24px 48px 0 rgba(145, 158, 171, 0.16)',
    tooltip: '0 4px 16px 0 rgba(145, 158, 171, 0.12)',
  },
  components,
};

export const theme = createTheme(themeOptions);
export default theme;