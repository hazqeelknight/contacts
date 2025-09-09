import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Grid,
  Box,
  Typography,
} from '@mui/material';
import { useForm, Controller } from 'react-hook-form';
import { Button } from '@/components/core';
import { ContactGroup } from '../types';

interface ContactGroupFormProps {
  open: boolean;
  onClose: () => void;
  onSubmit: (data: Partial<ContactGroup>) => void;
  loading?: boolean;
  group?: ContactGroup | null;
}

interface ContactGroupFormData {
  name: string;
  description: string;
  color: string;
}

export const ContactGroupForm: React.FC<ContactGroupFormProps> = ({
  open,
  onClose,
  onSubmit,
  loading = false,
  group = null,
}) => {
  const {
    control,
    handleSubmit,
    formState: { errors },
    reset,
  } = useForm<ContactGroupFormData>({
    defaultValues: {
      name: group?.name || '',
      description: group?.description || '',
      color: group?.color || '#0066cc',
    },
  });

  React.useEffect(() => {
    if (group) {
      reset({
        name: group.name,
        description: group.description,
        color: group.color,
      });
    } else {
      reset({
        name: '',
        description: '',
        color: '#0066cc',
      });
    }
  }, [group, reset]);

  const handleFormSubmit = (data: ContactGroupFormData) => {
    onSubmit(data);
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>
        {group ? 'Edit Contact Group' : 'Create Contact Group'}
      </DialogTitle>
      
      <DialogContent>
        <Box component="form" sx={{ mt: 2 }}>
          <Grid container spacing={2}>
            <Grid item xs={12}>
              <Controller
                name="name"
                control={control}
                rules={{ required: 'Group name is required' }}
                render={({ field }) => (
                  <TextField
                    {...field}
                    fullWidth
                    label="Group Name"
                    error={!!errors.name}
                    helperText={errors.name?.message}
                  />
                )}
              />
            </Grid>
            
            <Grid item xs={12}>
              <Controller
                name="description"
                control={control}
                render={({ field }) => (
                  <TextField
                    {...field}
                    fullWidth
                    label="Description"
                    multiline
                    rows={3}
                  />
                )}
              />
            </Grid>
            
            <Grid item xs={12}>
              <Typography variant="subtitle2" gutterBottom>
                Color
              </Typography>
              <Controller
                name="color"
                control={control}
                render={({ field }) => (
                  <Box display="flex" alignItems="center" gap={2}>
                    <input
                      type="color"
                      {...field}
                      style={{
                        width: 50,
                        height: 40,
                        border: 'none',
                        borderRadius: 4,
                        cursor: 'pointer',
                      }}
                    />
                    <TextField
                      value={field.value}
                      onChange={field.onChange}
                      size="small"
                      placeholder="#0066cc"
                      sx={{ width: 120 }}
                    />
                  </Box>
                )}
              />
            </Grid>
          </Grid>
        </Box>
      </DialogContent>
      
      <DialogActions>
        <Button onClick={onClose} variant="outlined">
          Cancel
        </Button>
        <Button
          onClick={handleSubmit(handleFormSubmit)}
          variant="contained"
          loading={loading}
        >
          {group ? 'Update Group' : 'Create Group'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};