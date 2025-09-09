import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { api } from '@/api/client';
import { queryKeys } from '@/api/queryClient';
import { toast } from 'react-toastify';
import { Contact, ContactGroup, ContactInteraction, ContactFilters, ContactStats } from '../types';
import { PaginatedResponse } from '@/types';

// Contact API hooks
export const useContacts = (filters?: ContactFilters, page = 0, pageSize = 20) => {
  return useQuery({
    queryKey: queryKeys.contacts.list({ ...filters, page, pageSize }),
    queryFn: async () => {
      const params = {
        page: page + 1, // Backend uses 1-based pagination
        page_size: pageSize,
        ...filters,
      };
      const response = await api.get<PaginatedResponse<Contact>>('/contacts/', { params });
      return response.data;
    },
  });
};

export const useContact = (id: string) => {
  return useQuery({
    queryKey: queryKeys.contacts.contact(id),
    queryFn: async () => {
      const response = await api.get<Contact>(`/contacts/${id}/`);
      return response.data;
    },
    enabled: !!id,
  });
};

export const useCreateContact = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async (data: Partial<Contact>) => {
      const response = await api.post<Contact>('/contacts/', data);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.all });
      toast.success('Contact created successfully');
    },
  });
};

export const useUpdateContact = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async ({ id, data }: { id: string; data: Partial<Contact> }) => {
      const response = await api.patch<Contact>(`/contacts/${id}/`, data);
      return response.data;
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.all });
      queryClient.setQueryData(queryKeys.contacts.contact(data.id), data);
      toast.success('Contact updated successfully');
    },
  });
};

export const useDeleteContact = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async (id: string) => {
      await api.delete(`/contacts/${id}/`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.all });
      toast.success('Contact deleted successfully');
    },
  });
};

// Contact Groups API hooks
export const useContactGroups = () => {
  return useQuery({
    queryKey: queryKeys.contacts.groups(),
    queryFn: async () => {
      const response = await api.get<PaginatedResponse<ContactGroup>>('/contacts/groups/');
      return response.data;
    },
  });
};

export const useContactGroup = (id: string) => {
  return useQuery({
    queryKey: queryKeys.contacts.groups(),
    queryFn: async () => {
      const response = await api.get<ContactGroup>(`/contacts/groups/${id}/`);
      return response.data;
    },
    enabled: !!id,
  });
};

export const useCreateContactGroup = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async (data: Partial<ContactGroup>) => {
      const response = await api.post<ContactGroup>('/contacts/groups/', data);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.groups() });
      toast.success('Contact group created successfully');
    },
  });
};

export const useUpdateContactGroup = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async ({ id, data }: { id: string; data: Partial<ContactGroup> }) => {
      const response = await api.patch<ContactGroup>(`/contacts/groups/${id}/`, data);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.groups() });
      toast.success('Contact group updated successfully');
    },
  });
};

export const useDeleteContactGroup = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async (id: string) => {
      await api.delete(`/contacts/groups/${id}/`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.groups() });
      toast.success('Contact group deleted successfully');
    },
  });
};

// Contact Interactions API hooks
export const useContactInteractions = (contactId?: string, page = 0, pageSize = 20, filters?: { interaction_type?: string }) => {
  return useQuery({
    queryKey: queryKeys.contacts.interactions(contactId),
    queryFn: async () => {
      const url = contactId ? `/contacts/${contactId}/interactions/` : '/contacts/interactions/';
      const params = {
        page: page + 1,
        page_size: pageSize,
        ...filters,
      };
      const response = await api.get<PaginatedResponse<ContactInteraction>>(url, { params });
      return response.data;
    },
  });
};

export const useAddContactInteraction = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async ({ contactId, data }: { contactId: string; data: Partial<ContactInteraction> }) => {
      const response = await api.post<ContactInteraction>(`/contacts/${contactId}/interactions/add/`, data);
      return response.data;
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.interactions(variables.contactId) });
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.interactions() });
      toast.success('Interaction added successfully');
    },
  });
};

// Group Management API hooks
export const useAddContactToGroup = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async ({ contactId, groupId }: { contactId: string; groupId: string }) => {
      const response = await api.post(`/contacts/${contactId}/groups/${groupId}/add/`);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.all });
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.groups() });
      toast.success('Contact added to group');
    },
  });
};

export const useRemoveContactFromGroup = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async ({ contactId, groupId }: { contactId: string; groupId: string }) => {
      const response = await api.post(`/contacts/${contactId}/groups/${groupId}/remove/`);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.all });
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.groups() });
      toast.success('Contact removed from group');
    },
  });
};

// Statistics API hooks
export const useContactStats = () => {
  return useQuery({
    queryKey: queryKeys.contacts.stats(),
    queryFn: async () => {
      const response = await api.get<ContactStats>('/contacts/stats/');
      return response.data;
    },
  });
};

// Import/Export API hooks
export const useImportContacts = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async (data: { csv_file: File; skip_duplicates: boolean; update_existing: boolean }) => {
      const formData = new FormData();
      formData.append('csv_file', data.csv_file);
      formData.append('skip_duplicates', data.skip_duplicates.toString());
      formData.append('update_existing', data.update_existing.toString());
      
      const response = await api.post('/contacts/import/', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      });
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.all });
      toast.success('Contact import started');
    },
  });
};

export const useExportContacts = () => {
  return useMutation({
    mutationFn: async () => {
      const response = await api.get('/contacts/export/', {
        responseType: 'blob',
      });
      
      // Create download link
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', 'contacts.csv');
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
      
      return response.data;
    },
    onSuccess: () => {
      toast.success('Contacts exported successfully');
    },
  });
};

// Merge Contacts API hooks
export const useMergeContacts = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async (data: { primary_contact_id: string; duplicate_contact_ids: string[] }) => {
      const response = await api.post('/contacts/merge/', data);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.contacts.all });
      toast.success('Contact merge initiated');
    },
  });
};

// Task Status API hook
export const useTaskStatus = (taskId: string) => {
  return useQuery({
    queryKey: ['task-status', taskId],
    queryFn: async () => {
      const response = await api.get(`/contacts/tasks/${taskId}/status/`);
      return response.data;
    },
    enabled: !!taskId,
    refetchInterval: (data) => {
      // Stop polling if task is completed
      if (data?.status === 'SUCCESS' || data?.status === 'FAILURE') {
        return false;
      }
      return 2000; // Poll every 2 seconds
    },
  });
};