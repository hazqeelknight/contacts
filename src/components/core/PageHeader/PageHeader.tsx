import React from 'react';
import { Box, Typography, Breadcrumbs, Link } from '@mui/material';
import { motion } from 'framer-motion';
import { NavigateNext } from '@mui/icons-material';

interface BreadcrumbItem {
  label: string;
  href?: string;
  onClick?: () => void;
}

interface PageHeaderProps {
  title: string;
  subtitle?: string;
  breadcrumbs?: BreadcrumbItem[];
  actions?: React.ReactNode;
  children?: React.ReactNode;
}

export const PageHeader: React.FC<PageHeaderProps> = ({
  title,
  subtitle,
  breadcrumbs,
  actions,
  children,
}) => {
  return (
    <motion.div
      initial={{ opacity: 0, y: -20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: 'easeOut' }}
    >
      <Box sx={{ mb: 4 }}>
        {breadcrumbs && breadcrumbs.length > 0 && (
          <Breadcrumbs
            separator={<NavigateNext fontSize="small" />}
            sx={{ mb: 2 }}
          >
            {breadcrumbs.map((item, index) => (
              <Link
                key={index}
                color={index === breadcrumbs.length - 1 ? 'text.primary' : 'inherit'}
                href={item.href}
                onClick={item.onClick}
                sx={{
                  textDecoration: 'none',
                  '&:hover': {
                    textDecoration: index === breadcrumbs.length - 1 ? 'none' : 'underline',
                  },
                }}
              >
                {item.label}
              </Link>
            ))}
          </Breadcrumbs>
        )}
        
        <Box
          display="flex"
          justifyContent="space-between"
          alignItems="flex-start"
          flexWrap="wrap"
          gap={2}
        >
          <Box>
            <Typography variant="h3" component="h1" gutterBottom>
              {title}
            </Typography>
            {subtitle && (
              <Typography variant="body1" color="text.secondary">
                {subtitle}
              </Typography>
            )}
          </Box>
          
          {actions && (
            <Box display="flex" gap={1} alignItems="center">
              {actions}
            </Box>
          )}
        </Box>
        
        {children}
      </Box>
    </motion.div>
  );
};