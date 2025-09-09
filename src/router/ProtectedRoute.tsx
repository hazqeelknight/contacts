import React from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { useAuthStore } from '@/store/authStore';
import { LoadingSpinner } from '@/components/core';

interface ProtectedRouteProps {
  children: React.ReactNode;
  requiredPermission?: string;
}

export const ProtectedRoute: React.FC<ProtectedRouteProps> = ({ 
  children, 
  requiredPermission 
}) => {
  const { isAuthenticated, user, isLoading } = useAuthStore();
  const location = useLocation();

  if (isLoading) {
    return <LoadingSpinner fullScreen message="Authenticating..." />;
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }

  // Handle various account statuses and email verification
  if (user) {
    // Priority 1: Password expiry/grace period
    if (user.account_status === 'password_expired' || user.account_status === 'password_expired_grace_period') {
      return <Navigate to="/change-password" replace />;
    }

    // Priority 2: Email verification (for pending_verification accounts or active but unverified emails)
    if (user.account_status === 'pending_verification' || !user.is_email_verified) {
      return <Navigate to="/verify-email" replace />;
    }

    // Priority 3: Other non-active statuses (suspended, inactive, etc.)
    if (user.account_status !== 'active') {
      return <Navigate to="/login" replace />;
    }
  }

  // Check permissions if required (only if user is authenticated and account status is 'active' and email verified)
  if (requiredPermission && user) {
    const hasPermission = user.roles.some(role =>
      role.role_permissions.some(permission => permission.codename === requiredPermission)
    );
    
    if (!hasPermission) {
      return <Navigate to="/dashboard" replace />;
    }
  }

  return <>{children}</>;
};