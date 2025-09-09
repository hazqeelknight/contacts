// 8px spacing system for consistent layouts
export const spacing = (factor: number) => `${8 * factor}px`;

// Predefined spacing values for common use cases
export const spacingValues = {
  xs: 4,    // 4px
  sm: 8,    // 8px
  md: 16,   // 16px
  lg: 24,   // 24px
  xl: 32,   // 32px
  xxl: 48,  // 48px
  xxxl: 64, // 64px
} as const;