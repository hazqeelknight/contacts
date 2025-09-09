import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  FormControlLabel,
  Checkbox,
  Box,
  Typography,
  Alert,
  LinearProgress,
  Paper,
} from '@mui/material';
import { CloudUpload, Description } from '@mui/icons-material';
import { useForm, Controller } from 'react-hook-form';
import { Button } from '@/components/core';
import { useTaskStatus } from '../api';

interface ContactImportModalProps {
  open: boolean;
  onClose: () => void;
  onSubmit: (data: { csv_file: File; skip_duplicates: boolean; update_existing: boolean }) => void;
  loading?: boolean;
  taskId?: string | null;
}

interface ImportFormData {
  csv_file: FileList;
  skip_duplicates: boolean;
  update_existing: boolean;
}

export const ContactImportModal: React.FC<ContactImportModalProps> = ({
  open,
  onClose,
  onSubmit,
  loading = false,
  taskId = null,
}) => {
  const [dragOver, setDragOver] = React.useState(false);
  
  const { data: taskStatus } = useTaskStatus(taskId || '');
  
  const {
    control,
    handleSubmit,
    formState: { errors },
    reset,
    setValue,
    watch,
  } = useForm<ImportFormData>({
    defaultValues: {
      skip_duplicates: true,
      update_existing: false,
    },
  });

  const csvFile = watch('csv_file');

  const handleFormSubmit = (data: ImportFormData) => {
    if (data.csv_file && data.csv_file[0]) {
      onSubmit({
        csv_file: data.csv_file[0],
        skip_duplicates: data.skip_duplicates,
        update_existing: data.update_existing,
      });
    }
  };

  const handleClose = () => {
    reset();
    onClose();
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(true);
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    
    const files = e.dataTransfer.files;
    if (files.length > 0 && files[0].type === 'text/csv') {
      const fileList = Array.from(files) as any;
      setValue('csv_file', fileList);
    }
  };

  React.useEffect(() => {
    if (taskStatus?.status === 'SUCCESS' || taskStatus?.status === 'FAILURE') {
      // Task completed, close modal after a delay
      setTimeout(() => {
        handleClose();
      }, 2000);
    }
  }, [taskStatus]);

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle>Import Contacts</DialogTitle>
      
      <DialogContent>
        <Box sx={{ mt: 2 }}>
          {taskId && taskStatus ? (
            <Box>
              <Typography variant="h6" gutterBottom>
                Import Progress
              </Typography>
              
              {taskStatus.status === 'PENDING' || taskStatus.status === 'STARTED' ? (
                <Box>
                  <LinearProgress sx={{ mb: 2 }} />
                  <Typography variant="body2" color="text.secondary">
                    Processing your CSV file...
                  </Typography>
                </Box>
              ) : taskStatus.status === 'SUCCESS' ? (
                <Alert severity="success">
                  {taskStatus.result?.message || 'Import completed successfully'}
                  {taskStatus.result?.created_count !== undefined && (
                    <Typography variant="body2" sx={{ mt: 1 }}>
                      Created: {taskStatus.result.created_count}, 
                      Updated: {taskStatus.result.updated_count || 0}, 
                      Skipped: {taskStatus.result.skipped_count || 0}
                    </Typography>
                  )}
                </Alert>
              ) : (
                <Alert severity="error">
                  {taskStatus.error || 'Import failed'}
                </Alert>
              )}
            </Box>
          ) : (
            <Box component="form">
              {/* File Upload Area */}
              <Paper
                sx={{
                  p: 3,
                  border: '2px dashed',
                  borderColor: dragOver ? 'primary.main' : 'grey.300',
                  backgroundColor: dragOver ? 'action.hover' : 'background.paper',
                  textAlign: 'center',
                  cursor: 'pointer',
                  mb: 3,
                  transition: 'all 0.2s ease',
                }}
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
                onClick={() => document.getElementById('csv-file-input')?.click()}
              >
                <CloudUpload sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
                <Typography variant="h6" gutterBottom>
                  {csvFile && csvFile[0] ? csvFile[0].name : 'Drop CSV file here or click to browse'}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Supported format: CSV files only
                </Typography>
                
                <Controller
                  name="csv_file"
                  control={control}
                  rules={{ required: 'Please select a CSV file' }}
                  render={({ field: { onChange, value, ...field } }) => (
                    <input
                      {...field}
                      id="csv-file-input"
                      type="file"
                      accept=".csv"
                      style={{ display: 'none' }}
                      onChange={(e) => onChange(e.target.files)}
                    />
                  )}
                />
              </Paper>

              {errors.csv_file && (
                <Alert severity="error" sx={{ mb: 2 }}>
                  {errors.csv_file.message}
                </Alert>
              )}

              {/* Import Options */}
              <Typography variant="subtitle2" gutterBottom>
                Import Options
              </Typography>
              
              <Controller
                name="skip_duplicates"
                control={control}
                render={({ field }) => (
                  <FormControlLabel
                    control={<Checkbox {...field} checked={field.value} />}
                    label="Skip duplicate contacts (based on email)"
                    sx={{ mb: 1 }}
                  />
                )}
              />
              
              <Controller
                name="update_existing"
                control={control}
                render={({ field }) => (
                  <FormControlLabel
                    control={<Checkbox {...field} checked={field.value} />}
                    label="Update existing contacts with new information"
                  />
                )}
              />

              {/* CSV Format Help */}
              <Alert severity="info" sx={{ mt: 2 }}>
                <Typography variant="subtitle2" gutterBottom>
                  Expected CSV Format:
                </Typography>
                <Typography variant="body2" component="div">
                  Headers: first_name, last_name, email, phone, company, job_title, notes, tags
                  <br />
                  • Email is required for all contacts
                  • Tags should be comma-separated within the cell
                </Typography>
              </Alert>
            </Box>
          )}
        </Box>
      </DialogContent>
      
      <DialogActions>
        <Button onClick={handleClose} variant="outlined">
          {taskId && (taskStatus?.status === 'PENDING' || taskStatus?.status === 'STARTED') ? 'Cancel' : 'Close'}
        </Button>
        {!taskId && (
          <Button
            onClick={handleSubmit(handleFormSubmit)}
            variant="contained"
            loading={loading}
            disabled={!csvFile || !csvFile[0]}
          >
            Import Contacts
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};