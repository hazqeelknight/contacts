import { TypographyOptions } from '@mui/material/styles/createTypography';

export const typography: TypographyOptions = {
  fontFamily: [
    'Inter',
    '-apple-system',
    'BlinkMacSystemFont',
    '"Segoe UI"',
    'Roboto',
    '"Helvetica Neue"',
    'Arial',
    'sans-serif',
    '"Apple Color Emoji"',
    '"Segoe UI Emoji"',
    '"Segoe UI Symbol"',
  ].join(','),
  fontWeightLight: 300,
  fontWeightRegular: 400,
  fontWeightMedium: 500,
  fontWeightBold: 600,
  h1: {
    fontWeight: 700,
    fontSize: '2.5rem',
    lineHeight: 1.2,
    letterSpacing: '-0.02em',
  },
  h2: {
    fontWeight: 600,
    fontSize: '2rem',
    lineHeight: 1.3,
    letterSpacing: '-0.01em',
  },
  h3: {
    fontWeight: 600,
    fontSize: '1.5rem',
    lineHeight: 1.4,
    letterSpacing: '-0.01em',
  },
  h4: {
    fontWeight: 600,
    fontSize: '1.25rem',
    lineHeight: 1.4,
  },
  h5: {
    fontWeight: 600,
    fontSize: '1.125rem',
    lineHeight: 1.5,
  },
  h6: {
    fontWeight: 600,
    fontSize: '1rem',
    lineHeight: 1.5,
  },
  subtitle1: {
    fontWeight: 500,
    fontSize: '1rem',
    lineHeight: 1.5,
  },
  subtitle2: {
    fontWeight: 500,
    fontSize: '0.875rem',
    lineHeight: 1.5,
  },
  body1: {
    fontWeight: 400,
    fontSize: '1rem',
    lineHeight: 1.6,
  },
  body2: {
    fontWeight: 400,
    fontSize: '0.875rem',
    lineHeight: 1.6,
  },
  caption: {
    fontWeight: 400,
    fontSize: '0.75rem',
    lineHeight: 1.5,
  },
  overline: {
    fontWeight: 600,
    fontSize: '0.75rem',
    lineHeight: 1.5,
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
  },
  button: {
    fontWeight: 500,
    fontSize: '0.875rem',
    lineHeight: 1.5,
    textTransform: 'none',
    letterSpacing: '0.02em',
  },
};