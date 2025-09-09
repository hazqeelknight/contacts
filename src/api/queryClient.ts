import { QueryClient } from '@tanstack/react-query';
import { toast } from 'react-toastify';

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: (failureCount, error: any) => {
        // Don't retry on 4xx errors (client errors)
        if (error?.response?.status >= 400 && error?.response?.status < 500) {
          return false;
        }
        // Retry up to 3 times for other errors
        return failureCount < 3;
      },
      staleTime: 5 * 60 * 1000, // 5 minutes
      gcTime: 10 * 60 * 1000, // 10 minutes (formerly cacheTime)
      refetchOnWindowFocus: false,
      refetchOnReconnect: true,
    },
    mutations: {
      onError: (error: any) => {
        // Global error handling for mutations
        if (error?.response?.data?.error) {
          toast.error(error.response.data.error);
        } else if (error?.message) {
          toast.error(error.message);
        } else {
          toast.error('An unexpected error occurred');
        }
      },
    },
  },
});

// Query keys factory for consistent cache management
export const queryKeys = {
  // Users module
  users: {
    all: ['users'] as const,
    profile: () => [...queryKeys.users.all, 'profile'] as const,
    sessions: () => [...queryKeys.users.all, 'sessions'] as const,
    auditLogs: () => [...queryKeys.users.all, 'audit-logs'] as const,
    roles: () => [...queryKeys.users.all, 'roles'] as const,
    permissions: () => [...queryKeys.users.all, 'permissions'] as const,
  },
  
  // Events module
  events: {
    all: ['events'] as const,
    eventTypes: () => [...queryKeys.events.all, 'event-types'] as const,
    eventType: (id: string) => [...queryKeys.events.eventTypes(), id] as const,
    bookings: (filters?: Record<string, any>) => [...queryKeys.events.all, 'bookings', filters] as const,
    booking: (id: string) => [...queryKeys.events.all, 'booking', id] as const,
    publicOrganizer: (slug: string) => [...queryKeys.events.all, 'public', slug] as const,
    publicEventType: (organizerSlug: string, eventSlug: string) => 
      [...queryKeys.events.all, 'public', organizerSlug, eventSlug] as const,
    availableSlots: (organizerSlug: string, eventSlug: string, params: Record<string, any>) =>
      [...queryKeys.events.all, 'slots', organizerSlug, eventSlug, params] as const,
    analytics: () => [...queryKeys.events.all, 'analytics'] as const,
  },
  
  // Availability module
  availability: {
    all: ['availability'] as const,
    rules: () => [...queryKeys.availability.all, 'rules'] as const,
    overrides: () => [...queryKeys.availability.all, 'overrides'] as const,
    blockedTimes: () => [...queryKeys.availability.all, 'blocked'] as const,
    recurringBlocks: () => [...queryKeys.availability.all, 'recurring-blocks'] as const,
    bufferSettings: () => [...queryKeys.availability.all, 'buffer'] as const,
    stats: () => [...queryKeys.availability.all, 'stats'] as const,
    calculatedSlots: (organizerSlug: string, params: Record<string, any>) =>
      [...queryKeys.availability.all, 'calculated-slots', organizerSlug, params] as const,
  },
  
  // Integrations module
  integrations: {
    all: ['integrations'] as const,
    calendar: () => [...queryKeys.integrations.all, 'calendar'] as const,
    video: () => [...queryKeys.integrations.all, 'video'] as const,
    webhooks: () => [...queryKeys.integrations.all, 'webhooks'] as const,
    logs: () => [...queryKeys.integrations.all, 'logs'] as const,
    health: () => [...queryKeys.integrations.all, 'health'] as const,
    conflicts: () => [...queryKeys.integrations.all, 'calendar', 'conflicts'] as const,
  },
  
  // Notifications module
  notifications: {
    all: ['notifications'] as const,
    templates: () => [...queryKeys.notifications.all, 'templates'] as const,
    logs: () => [...queryKeys.notifications.all, 'logs'] as const,
    preferences: () => [...queryKeys.notifications.all, 'preferences'] as const,
    scheduled: () => [...queryKeys.notifications.all, 'scheduled'] as const,
    stats: () => [...queryKeys.notifications.all, 'stats'] as const,
    health: () => [...queryKeys.notifications.all, 'health'] as const,
  },
  
  // Contacts module
  contacts: {
    all: ['contacts'] as const,
    list: (filters?: Record<string, any>) => [...queryKeys.contacts.all, 'list', filters] as const,
    contact: (id: string) => [...queryKeys.contacts.all, 'contact', id] as const,
    groups: () => [...queryKeys.contacts.all, 'groups'] as const,
    interactions: (contactId?: string) => [...queryKeys.contacts.all, 'interactions', contactId] as const,
    stats: () => [...queryKeys.contacts.all, 'stats'] as const,
  },
  
  // Workflows module
  workflows: {
    all: ['workflows'] as const,
    list: () => [...queryKeys.workflows.all, 'list'] as const,
    workflow: (id: string) => [...queryKeys.workflows.all, 'workflow', id] as const,
    actions: (workflowId: string) => [...queryKeys.workflows.all, 'actions', workflowId] as const,
    executions: () => [...queryKeys.workflows.all, 'executions'] as const,
    templates: () => [...queryKeys.workflows.all, 'templates'] as const,
    performanceStats: () => [...queryKeys.workflows.all, 'performance-stats'] as const,
  },
} as const;