import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useAuthStore } from '@/store/authStore';
import { useUserAuth } from '@/users/hooks';
import { toast } from 'react-toastify';

/**
 * Legacy useAuth hook - now delegates to Users module
 * @deprecated Use useUserAuth from @/users/hooks instead
 */
export const useAuth = () => {
  // Delegate to the Users module implementation
  return useUserAuth();
};