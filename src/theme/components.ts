import { Components, Theme } from '@mui/material/styles';

export const components: Components<Omit<Theme, 'components'>> = {
  MuiButton: {
    styleOverrides: {
      root: {
        borderRadius: 8,
        textTransform: 'none',
        fontWeight: 500,
        fontSize: '0.875rem',
        padding: '10px 20px',
        boxShadow: 'none',
        transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)',
        '&:hover': {
          boxShadow: '0 4px 12px rgba(99, 102, 241, 0.15)',
          transform: 'translateY(-1px)',
        },
        '&:active': {
          transform: 'translateY(0)',
        },
      },
      contained: {
        '&:hover': {
          boxShadow: '0 6px 20px rgba(99, 102, 241, 0.25)',
        },
      },
      outlined: {
        borderWidth: '1.5px',
        '&:hover': {
          borderWidth: '1.5px',
          backgroundColor: 'rgba(99, 102, 241, 0.04)',
        },
      },
      text: {
        '&:hover': {
          backgroundColor: 'rgba(99, 102, 241, 0.04)',
        },
      },
    },
  },
  MuiCard: {
    styleOverrides: {
      root: {
        borderRadius: 16,
        border: '1px solid #E5E7EB',
        boxShadow: '0 2px 16px 0 rgba(145, 158, 171, 0.08)',
        transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)',
        '&:hover': {
          boxShadow: '0 8px 32px 0 rgba(145, 158, 171, 0.12)',
          transform: 'translateY(-2px)',
        },
      },
    },
  },
  MuiPaper: {
    styleOverrides: {
      root: {
        borderRadius: 12,
        border: '1px solid #E5E7EB',
      },
      elevation1: {
        boxShadow: '0 2px 16px 0 rgba(145, 158, 171, 0.08)',
      },
    },
  },
  MuiTextField: {
    styleOverrides: {
      root: {
        '& .MuiOutlinedInput-root': {
          borderRadius: 8,
          transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)',
          '&:hover .MuiOutlinedInput-notchedOutline': {
            borderColor: '#6366F1',
          },
          '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
            borderWidth: '2px',
            borderColor: '#6366F1',
          },
        },
      },
    },
  },
  MuiChip: {
    styleOverrides: {
      root: {
        borderRadius: 8,
        fontWeight: 500,
        fontSize: '0.75rem',
      },
      filled: {
        backgroundColor: 'rgba(99, 102, 241, 0.08)',
        color: '#6366F1',
        '&:hover': {
          backgroundColor: 'rgba(99, 102, 241, 0.12)',
        },
      },
    },
  },
  MuiAppBar: {
    styleOverrides: {
      root: {
        backgroundColor: '#FFFFFF',
        color: '#1F2937',
        boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06)',
        borderBottom: '1px solid #E5E7EB',
      },
    },
  },
  MuiDrawer: {
    styleOverrides: {
      paper: {
        borderRight: '1px solid #E5E7EB',
        backgroundColor: '#FAFBFC',
      },
    },
  },
  MuiListItemButton: {
    styleOverrides: {
      root: {
        borderRadius: 8,
        margin: '2px 8px',
        '&:hover': {
          backgroundColor: 'rgba(99, 102, 241, 0.04)',
        },
        '&.Mui-selected': {
          backgroundColor: 'rgba(99, 102, 241, 0.08)',
          color: '#6366F1',
          '&:hover': {
            backgroundColor: 'rgba(99, 102, 241, 0.12)',
          },
        },
      },
    },
  },
  MuiTab: {
    styleOverrides: {
      root: {
        textTransform: 'none',
        fontWeight: 500,
        fontSize: '0.875rem',
        minHeight: 48,
        '&.Mui-selected': {
          color: '#6366F1',
        },
      },
    },
  },
  MuiDialog: {
    styleOverrides: {
      paper: {
        borderRadius: 16,
        boxShadow: '0 24px 48px 0 rgba(145, 158, 171, 0.16)',
      },
    },
  },
  MuiTooltip: {
    styleOverrides: {
      tooltip: {
        backgroundColor: '#374151',
        fontSize: '0.75rem',
        borderRadius: 6,
        padding: '8px 12px',
      },
      arrow: {
        color: '#374151',
      },
    },
  },
};