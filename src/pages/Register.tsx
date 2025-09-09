import React from 'react';
import {
  Container,
  Card,
  CardContent,
  Typography,
  Box,
  TextField,
  FormControlLabel,
  Checkbox,
  Link,
  Divider,
  Grid,
  Alert,
} from '@mui/material';
import { PersonAdd, Login as LoginIcon } from '@mui/icons-material';
import { motion } from 'framer-motion';
import { useForm, Controller } from 'react-hook-form';
import { useNavigate } from 'react-router-dom';
import { Button } from '@/components/core';
import { useUserAuth } from '@/users/hooks';
import { RegisterData } from '@/users/types';
import { validateEmail, validatePassword } from '@/users/utils';

const Register: React.FC = () => {
  const navigate = useNavigate();
  const { register, isRegistering } = useUserAuth();

  const {
    control,
    handleSubmit,
    formState: { errors },
    watch,
    setError,
  } = useForm<RegisterData>({
    defaultValues: {
      email: '',
      first_name: '',
      last_name: '',
      password: '',
      password_confirm: '',
      terms_accepted: false,
    },
  });

  const password = watch('password');

  const handleFormSubmit = async (data: RegisterData) => {
    try {
      await register(data);
      navigate('/dashboard');
    } catch (error: any) {
      if (error?.response?.data?.errors) {
        // Handle field-specific errors
        Object.entries(error.response.data.errors).forEach(([field, messages]) => {
          setError(field as keyof RegisterData, { 
            message: Array.isArray(messages) ? messages[0] : messages 
          });
        });
      } else if (error?.response?.data?.error) {
        setError('root', { message: error.response.data.error });
      }
    }
  };

  return (
    <Container maxWidth="sm">
      <Box
        sx={{
          minHeight: '100vh',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          style={{ width: '100%' }}
        >
          <Card>
            <CardContent sx={{ p: 4 }}>
              <Box sx={{ textAlign: 'center', mb: 4 }}>
                <PersonAdd sx={{ fontSize: 60, color: 'primary.main', mb: 2 }} />
                <Typography variant="h4" gutterBottom>
                  Create Your Account
                </Typography>
                <Typography variant="body1" color="text.secondary">
                  Join NovaMeet and start scheduling smarter
                </Typography>
              </Box>

              {errors.root && (
                <Alert severity="error" sx={{ mb: 3 }}>
                  {errors.root.message}
                </Alert>
              )}

              <Box component="form" onSubmit={handleSubmit(handleFormSubmit)}>
                <Grid container spacing={2}>
                  <Grid item xs={6}>
                    <Controller
                      name="first_name"
                      control={control}
                      rules={{ required: 'First name is required' }}
                      render={({ field }) => (
                        <TextField
                          {...field}
                          fullWidth
                          label="First Name"
                          margin="normal"
                          error={!!errors.first_name}
                          helperText={errors.first_name?.message}
                          autoFocus
                        />
                      )}
                    />
                  </Grid>
                  <Grid item xs={6}>
                    <Controller
                      name="last_name"
                      control={control}
                      rules={{ required: 'Last name is required' }}
                      render={({ field }) => (
                        <TextField
                          {...field}
                          fullWidth
                          label="Last Name"
                          margin="normal"
                          error={!!errors.last_name}
                          helperText={errors.last_name?.message}
                        />
                      )}
                    />
                  </Grid>
                </Grid>

                <Controller
                  name="email"
                  control={control}
                  rules={{
                    required: 'Email is required',
                    validate: (value) => validateEmail(value) || 'Please enter a valid email address',
                  }}
                  render={({ field }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Email Address"
                      type="email"
                      margin="normal"
                      error={!!errors.email}
                      helperText={errors.email?.message}
                    />
                  )}
                />

                <Controller
                  name="password"
                  control={control}
                  rules={{
                    required: 'Password is required',
                    validate: (value) => {
                      const validation = validatePassword(value);
                      return validation.isValid || validation.errors[0];
                    },
                  }}
                  render={({ field }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Password"
                      type="password"
                      margin="normal"
                      error={!!errors.password}
                      helperText={errors.password?.message}
                    />
                  )}
                />

                <Controller
                  name="password_confirm"
                  control={control}
                  rules={{
                    required: 'Please confirm your password',
                    validate: (value) =>
                      value === password || 'Passwords do not match',
                  }}
                  render={({ field }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Confirm Password"
                      type="password"
                      margin="normal"
                      error={!!errors.password_confirm}
                      helperText={errors.password_confirm?.message}
                    />
                  )}
                />

                <Controller
                  name="terms_accepted"
                  control={control}
                  rules={{ required: 'You must accept the terms and conditions' }}
                  render={({ field }) => (
                    <FormControlLabel
                      control={
                        <Checkbox 
                          {...field} 
                          checked={field.value}
                          color={errors.terms_accepted ? 'error' : 'primary'}
                        />
                      }
                      label={
                        <Typography variant="body2">
                          I agree to the{' '}
                          <Link href="/terms" target="_blank">
                            Terms of Service
                          </Link>{' '}
                          and{' '}
                          <Link href="/privacy" target="_blank">
                            Privacy Policy
                          </Link>
                        </Typography>
                      }
                      sx={{ mt: 2, mb: 1 }}
                    />
                  )}
                />
                {errors.terms_accepted && (
                  <Typography variant="caption" color="error" sx={{ ml: 4 }}>
                    {errors.terms_accepted.message}
                  </Typography>
                )}

                <Button
                  type="submit"
                  fullWidth
                  variant="contained"
                  size="large"
                  loading={isRegistering}
                  loadingText="Creating account..."
                  sx={{ mt: 3, mb: 2 }}
                >
                  Create Account
                </Button>

                <Divider sx={{ my: 3 }}>
                  <Typography variant="body2" color="text.secondary">
                    Already have an account?
                  </Typography>
                </Divider>

                <Button
                  fullWidth
                  variant="outlined"
                  size="large"
                  startIcon={<LoginIcon />}
                  onClick={() => navigate('/login')}
                >
                  Sign In Instead
                </Button>
              </Box>
            </CardContent>
          </Card>
        </motion.div>
      </Box>
    </Container>
  );
};

export default Register;