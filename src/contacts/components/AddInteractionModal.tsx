import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  MenuItem,
  Box,
  Typography,
} from '@mui/material';
import { useForm, Controller } from 'react-hook-form';
import { Button } from '@/components/core';

interface AddInteractionModalProps {
  open: boolean;
  onClose: () => void;
  contactName: string;
  onSubmit: (data: { interaction_type: string; description: string; metadata?: Record<string, any> }) => void;
  loading?: boolean;
}

interface InteractionFormData {
  interaction_type: string;
  description: string;
}

const INTERACTION_TYPES = [
  { value: 'email_sent', label: 'Email Sent' },
  { value: 'note_added', label: 'Note Added' },
  { value: 'manual_entry', label: 'Manual Entry' },
  { value: 'phone_call', label: 'Phone Call' },
  { value: 'meeting', label: 'Meeting' },
];

export const AddInteractionModal: React.FC<AddInteractionModalProps> = ({
  open,
  onClose,
  contactName,
  onSubmit,
  loading = false,
}) => {
  const {
    control,
    handleSubmit,
    formState: { errors },
    reset,
  } = useForm<InteractionFormData>({
    defaultValues: {
      interaction_type: 'manual_entry',
      description: '',
    },
  });

  const handleFormSubmit = (data: InteractionFormData) => {
    onSubmit({
      interaction_type: data.interaction_type,
      description: data.description,
      metadata: {
        added_via: 'manual_ui',
        timestamp: new Date().toISOString(),
      },
    });
  };

  const handleClose = () => {
    reset();
    onClose();
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle>Add Interaction</DialogTitle>
      
      <DialogContent>
        <Box sx={{ mt: 2 }}>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
            Adding interaction for: <strong>{contactName}</strong>
          </Typography>

          <Controller
            name="interaction_type"
            control={control}
            rules={{ required: 'Interaction type is required' }}
            render={({ field }) => (
              <TextField
                {...field}
                select
                fullWidth
                label="Interaction Type"
                margin="normal"
                error={!!errors.interaction_type}
                helperText={errors.interaction_type?.message}
              >
                {INTERACTION_TYPES.map((type) => (
                  <MenuItem key={type.value} value={type.value}>
                    {type.label}
                  </MenuItem>
                ))}
              </TextField>
            )}
          />

          <Controller
            name="description"
            control={control}
            rules={{ required: 'Description is required' }}
            render={({ field }) => (
              <TextField
                {...field}
                fullWidth
                label="Description"
                multiline
                rows={4}
                margin="normal"
                error={!!errors.description}
                helperText={errors.description?.message}
                placeholder="Describe the interaction..."
              />
            )}
          />
        </Box>
      </DialogContent>
      
      <DialogActions>
        <Button onClick={handleClose} variant="outlined">
          Cancel
        </Button>
        <Button
          onClick={handleSubmit(handleFormSubmit)}
          variant="contained"
          loading={loading}
        >
          Add Interaction
        </Button>
      </DialogActions>
    </Dialog>
  );
};