import React from 'react';
import { Card as MuiCard, CardProps as MuiCardProps } from '@mui/material';
import { motion } from 'framer-motion';

export interface CardProps extends MuiCardProps {
  animate?: boolean;
  hover?: boolean;
}

const MotionCard = motion(MuiCard);

export const Card: React.FC<CardProps> = ({
  children,
  animate = true,
  hover = true,
  ...props
}) => {
  const CardComponent = animate ? MotionCard : MuiCard;
  
  const motionProps = animate ? {
    initial: { opacity: 0, y: 20 },
    animate: { opacity: 1, y: 0 },
    transition: { duration: 0.3, ease: 'easeOut' },
    ...(hover && {
      whileHover: { y: -4 },
      transition: { duration: 0.2 },
    }),
  } : {};

  return (
    <CardComponent
      {...props}
      {...motionProps}
    >
      {children}
    </CardComponent>
  );
};