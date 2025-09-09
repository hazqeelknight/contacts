import React from 'react';
import { Button as MuiButton, ButtonProps as MuiButtonProps, CircularProgress } from '@mui/material';
import { motion } from 'framer-motion';

export interface ButtonProps extends Omit<MuiButtonProps, 'component'> {
  loading?: boolean;
  loadingText?: string;
  animate?: boolean;
}

const MotionButton = motion(MuiButton);

export const Button: React.FC<ButtonProps> = ({
  children,
  loading = false,
  loadingText,
  disabled,
  animate = true,
  ...props
}) => {
  const ButtonComponent = animate ? MotionButton : MuiButton;
  
  const motionProps = animate ? {
    whileHover: { scale: 1.02 },
    whileTap: { scale: 0.98 },
    transition: { type: 'spring', stiffness: 400, damping: 17 },
  } : {};

  return (
    <ButtonComponent
      {...props}
      {...motionProps}
      disabled={disabled || loading}
      startIcon={loading ? <CircularProgress size={16} color="inherit" /> : props.startIcon}
    >
      {loading && loadingText ? loadingText : children}
    </ButtonComponent>
  );
};