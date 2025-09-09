import React from 'react';
import { createBrowserRouter, Navigate } from 'react-router-dom';
import { Layout } from '@/components/layout';
import { ProtectedRoute } from './ProtectedRoute';
import { PublicRoute } from './PublicRoute';

// Lazy load pages for better performance
const Dashboard = React.lazy(() => import('@/pages/Dashboard'));
const Login = React.lazy(() => import('@/pages/Login'));
const Register = React.lazy(() => import('@/pages/Register'));
const NotFound = React.lazy(() => import('@/pages/NotFound'));

// Module routes (to be imported from each module)
const usersRoutes = React.lazy(() => import('@/users/routes'));
const eventsRoutes = React.lazy(() => import('@/events/routes'));
const availabilityRoutes = React.lazy(() => import('@/availability/routes'));
const integrationsRoutes = React.lazy(() => import('@/integrations/routes'));
const notificationsRoutes = React.lazy(() => import('@/notifications/routes'));
const contactsRoutes = React.lazy(() => import('@/contacts/routes'));
const workflowsRoutes = React.lazy(() => import('@/workflows/routes'));

// Users module public routes
const VerifyEmail = React.lazy(() => import('@/users/pages/VerifyEmail'));
const RequestPasswordReset = React.lazy(() => import('@/users/pages/RequestPasswordReset'));
const ConfirmPasswordReset = React.lazy(() => import('@/users/pages/ConfirmPasswordReset'));
const ChangePassword = React.lazy(() => import('@/users/pages/ChangePassword'));
const RespondToInvitation = React.lazy(() => import('@/users/pages/RespondToInvitation'));
const PublicProfile = React.lazy(() => import('@/users/pages/PublicProfile'));

export const router = createBrowserRouter([
  // Public routes
  {
    path: '/login',
    element: (
      <PublicRoute>
        <Login />
      </PublicRoute>
    ),
  },
  {
    path: '/register',
    element: (
      <PublicRoute>
        <Register />
      </PublicRoute>
    ),
  },
  
  // Public authentication routes
  {
    path: '/verify-email',
    element: (
      <React.Suspense fallback={<div>Loading...</div>}>
        <VerifyEmail />
      </React.Suspense>
    ),
  },
  {
    path: '/request-password-reset',
    element: (
      <PublicRoute>
        <RequestPasswordReset />
      </PublicRoute>
    ),
  },
  {
    path: '/reset-password',
    element: (
      <PublicRoute>
        <ConfirmPasswordReset />
      </PublicRoute>
    ),
  },
  {
    path: '/change-password',
    element: (
      <React.Suspense fallback={<div>Loading...</div>}>
        <ChangePassword />
      </React.Suspense>
    ),
  },
  {
    path: '/invitation',
    element: (
      <PublicRoute>
        <RespondToInvitation />
      </PublicRoute>
    ),
  },
  
  // Public profile route
  {
    path: '/p/:organizerSlug',
    element: (
      <React.Suspense fallback={<div>Loading...</div>}>
        <PublicProfile />
      </React.Suspense>
    ),
  },
  
  // Protected routes with layout
  {
    path: '/',
    element: (
      <ProtectedRoute>
        <Layout>
          <React.Suspense fallback={<div>Loading...</div>}>
            <Dashboard />
          </React.Suspense>
        </Layout>
      </ProtectedRoute>
    ),
  },
  {
    path: '/dashboard',
    element: <Navigate to="/" replace />,
  },
  
  // Settings redirect to user profile
  {
    path: '/settings',
    element: (
      <ProtectedRoute>
        <Navigate to="/users/profile" replace />
      </ProtectedRoute>
    ),
  },
  
  // Module routes
  {
    path: '/users/*',
    element: (
      <ProtectedRoute>
        <Layout>
          <React.Suspense fallback={<div>Loading...</div>}>
            {React.createElement(usersRoutes)}
          </React.Suspense>
        </Layout>
      </ProtectedRoute>
    ),
  },
  {
    path: '/events/*',
    element: (
      <ProtectedRoute>
        <Layout>
          <React.Suspense fallback={<div>Loading...</div>}>
            {React.createElement(eventsRoutes)}
          </React.Suspense>
        </Layout>
      </ProtectedRoute>
    ),
  },
  {
    path: '/availability/*',
    element: (
      <ProtectedRoute>
        <Layout>
          <React.Suspense fallback={<div>Loading...</div>}>
            {React.createElement(availabilityRoutes)}
          </React.Suspense>
        </Layout>
      </ProtectedRoute>
    ),
  },
  {
    path: '/integrations/*',
    element: (
      <ProtectedRoute>
        <Layout>
          <React.Suspense fallback={<div>Loading...</div>}>
            {React.createElement(integrationsRoutes)}
          </React.Suspense>
        </Layout>
      </ProtectedRoute>
    ),
  },
  {
    path: '/notifications/*',
    element: (
      <ProtectedRoute>
        <Layout>
          <React.Suspense fallback={<div>Loading...</div>}>
            {React.createElement(notificationsRoutes)}
          </React.Suspense>
        </Layout>
      </ProtectedRoute>
    ),
  },
  {
    path: '/contacts/*',
    element: (
      <ProtectedRoute>
        <Layout>
          <React.Suspense fallback={<div>Loading...</div>}>
            {React.createElement(contactsRoutes)}
          </React.Suspense>
        </Layout>
      </ProtectedRoute>
    ),
  },
  {
    path: '/workflows/*',
    element: (
      <ProtectedRoute>
        <Layout>
          <React.Suspense fallback={<div>Loading...</div>}>
            {React.createElement(workflowsRoutes)}
          </React.Suspense>
        </Layout>
      </ProtectedRoute>
    ),
  },
  
  // 404 page
  {
    path: '*',
    element: <NotFound />,
  },
]);